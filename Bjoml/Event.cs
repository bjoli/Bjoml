// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
//
// This file is part of BjoML.
//
// BjoML is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// BjoML is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with BjoML.  If not, see <https://www.gnu.org/licenses/>.

namespace Bjoml;

using System;
using System.Threading.Tasks;

public interface IEvent<T>
{
    void Publish(SyncState sharedState, int eventId, Action<T> onSync);
}

// Dummy type for Unit since C# doesn't have it natively.
public struct Unit { }

public static class Cml
{
    // The core entrypoint for all CML operations
    public static void Sync<T>(IEvent<T> ev, Action<T> continuation)
    {
        var state = new SyncState();
        ev.Publish(state, SyncState.RootEventId, continuation);
    }

    public static ValueTask<T> SyncAsync<T>(IEvent<T> ev)
    {
        var source = CmlValueTaskSource<T>.Rent();
        Sync(ev, source.OnSyncDelegate);
        return new ValueTask<T>(source, source.Version);
    }

    public static ValueTask SyncAsyncVoid(IEvent<Unit> ev)
    {
        var source = CmlValueTaskSource<Unit>.Rent();
        Sync(ev, source.OnSyncDelegate);
        return new ValueTask(source, source.Version);
    }

    // Combinators
    public static IEvent<T> Choose<T>(params IEvent<T>[] events) => new ChooseEvent<T>(events);
    
    public static IEvent<U> Wrap<T, U>(IEvent<T> ev, Func<T, U> mapper) => new WrapEvent<T, U>(ev, mapper);
    
    public static IEvent<T> Guard<T>(Func<IEvent<T>> generator) => new GuardEvent<T>(generator);
    
    public static IEvent<T> WithNack<T>(Func<IEvent<Unit>, IEvent<T>> generator) => new WithNackEvent<T>(generator);
    
    public static IEvent<T> Always<T>(T value) => new AlwaysEvent<T>(value);
    
    public static IEvent<T> Never<T>() => new NeverEvent<T>();
}

// ---------------- Implementation of Combinators ----------------

/// <summary>
/// Combines multiple events into a single choice. 
/// In CML, 'choose' allows a thread to wait on multiple possible synchronizations simultaneously.
/// The first event to successfully claim the shared state wins, and all others are discarded.
/// </summary>
public class ChooseEvent<T> : IEvent<T>
{
    private readonly IEvent<T>[] _events;
    public ChooseEvent(params IEvent<T>[] events) { _events = events; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // Each branch gets a distinct sequential event index.
        // Hopac interval indexing: subtrees are tracked by range [I0, I1) on the SyncState.
        foreach (var ev in _events)
        {
            // An earlier branch may have committed inline (Always, an already-queued
            // partner, a completed Promise). Publishing the rest would only queue
            // operations that can never win.
            if (sharedState.IsSynchronized) return;

            ev.Publish(sharedState, sharedState.NextEventId(), onSync);
        }
    }
}

/// <summary>
/// Wraps an event to map its result.
/// Because ChooseEvent requires all its branches to return the same type T, Wrap is essential.
/// It allows you to combine differently-typed events (like receiving an Int vs a String) 
/// into a unified type before passing them to Choose.
/// </summary>
public class WrapEvent<T, U> : IEvent<U>
{
    private readonly IEvent<T> _ev;
    private readonly Func<T, U> _mapper;

    public WrapEvent(IEvent<T> ev, Func<T, U> mapper)
    {
        _ev = ev;
        _mapper = mapper;
    }

    public void Publish(SyncState sharedState, int eventId, Action<U> onSync)
    {
        // Intercept the synchronization callback to apply the mapping function before resuming the user.
        _ev.Publish(sharedState, eventId, value => onSync(_mapper(value)));
    }
}

/// <summary>
/// Delays the creation of an event until it is actually synchronized.
/// This is used when the event requires side-effects or dynamic state to be allocated 
/// only at the exact moment the thread commits to the synchronization block.
/// </summary>
public class GuardEvent<T> : IEvent<T>
{
    private readonly Func<IEvent<T>> _generator;

    public GuardEvent(Func<IEvent<T>> generator) { _generator = generator; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        var ev = _generator();
        ev.Publish(sharedState, eventId, onSync);
    }
}

/// <summary>
/// Negative Acknowledgement (NACK). 
/// When composing complex choices, a thread often needs to know if a specific branch LOST the race 
/// so it can abort tentative operations or clean up resources.
/// WithNack passes a NACK event (which fires if the branch loses) into a generator that builds the actual event.
/// </summary>
public class WithNackEvent<T> : IEvent<T>
{
    private readonly Func<IEvent<Unit>, IEvent<T>> _generator;

    public WithNackEvent(Func<IEvent<Unit>, IEvent<T>> generator)
    {
        _generator = generator;
    }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // The nack is carried by a Promise, not a Channel.
        //
        // A channel-based nack is EPHEMERAL: firing it parks a PutOp that only a
        // receiver can consume, so a nack nobody listens to leaves an operation
        // stranded in a channel that is then unreachable forever. A promise is
        // PERSISTENT, which is also the better semantics: "this branch lost" is a
        // fact, not a message, so every listener should see it and a listener that
        // arrives late should still see it.
        int i0 = eventId;
        var nack = new Promise<Unit>();

        // Register Nack interval starting at i0.
        var nackNode = sharedState.RegisterNack(i0, () => nack.TrySetResult(default));

        // A nack promise is only ever completed successfully, so the Result wrapper
        // can be projected away.
        var ev = _generator(Cml.Wrap(nack.Join(), static r => r.Value));
        
        // Publish the generated event with our starting eventId.
        ev.Publish(sharedState, eventId, onSync);

        // Subtree ends after the last minted branch index.
        int i1 = Math.Max(sharedState.CurrentEventId, i0 + 1);
        if (nackNode != null)
        {
            nackNode.I1 = i1;
            if (sharedState.IsSynchronized)
            {
                int winner = sharedState.WinningEventId;
                if (winner < i0 || i1 <= winner)
                {
                    nack.TrySetResult(default);
                }
            }
        }
    }
}

public class AlwaysEvent<T> : IEvent<T>
{
    private readonly T _value;
    public AlwaysEvent(T value) { _value = value; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // Commit outright, bypassing the queues entirely.
        //
        // TryCommit rather than TryClaim + MarkSynchronized: a bare TryClaim treats
        // a transient C (some other branch mid-pairing) as "already lost" and
        // silently drops an event that is always enabled.
        if (sharedState.TryCommit(eventId))
            Scheduler.Dispatch(onSync, _value);
    }
}

public class NeverEvent<T> : IEvent<T>
{
    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // Do nothing. It never synchronizes, mimicking an event that never occurs.
    }
}

// ---------------- Base Channel Events ----------------

public class ChannelSendEvent<T> : IEvent<Unit>
{
    private readonly Channel<T> _channel;
    private readonly T _value;

    public ChannelSendEvent(Channel<T> channel, T value)
    {
        _channel = channel;
        _value = value;
    }

    public void Publish(SyncState sharedState, int eventId, Action<Unit> onSync)
    {
        _channel.PublishSend(sharedState, eventId, _value, () => onSync(new Unit()));
    }
}

public class ChannelReceiveEvent<T> : IEvent<T>
{
    private readonly Channel<T> _channel;

    public ChannelReceiveEvent(Channel<T> channel)
    {
        _channel = channel;
    }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        _channel.PublishReceive(sharedState, eventId, onSync);
    }
}