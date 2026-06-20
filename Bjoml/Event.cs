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
        ev.Publish(state, state.GenerateEventId(), continuation);
    }

    public static Task<T> SyncAsync<T>(IEvent<T> ev)
    {
        var tcs = new TaskCompletionSource<T>();
        Sync(ev, value => tcs.SetResult(value));
        return tcs.Task;
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
        // For choose, we generate a NEW Event ID for each branch.
        // This is strictly necessary because if branch A wins, we MUST fire the NACKs for branch B and C.
        // By giving them different EventIDs under the same SyncState, the SyncState knows exactly which 
        // branch won and can fire the NACKs belonging to the losers.
        foreach (var ev in _events)
        {
            ev.Publish(sharedState, sharedState.GenerateEventId(), onSync);
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
        var nackChan = new Channel<Unit>();
        
        // We register the NACK callback on the shared state, bound specifically to OUR 'eventId'.
        // If the sharedState is synchronized by any other EventID, it will trigger this callback.
        // We use a completely independent SyncState (new SyncState()) for the nackChan's PublishSend 
        // because the NACK delivery is a distinct rendezvous.
        sharedState.RegisterNack(eventId, () => nackChan.PublishSend(new SyncState(), 1, new Unit(), () => { }));

        var ev = _generator(new ChannelReceiveEvent<Unit>(nackChan));
        
        // Publish the generated event WITH OUR EVENT ID. 
        // This means if `ev` wins, it identifies itself to the SyncState using our eventId, 
        // which tells the SyncState NOT to fire our registered NACK!
        ev.Publish(sharedState, eventId, onSync);
    }
}

public class AlwaysEvent<T> : IEvent<T>
{
    private readonly T _value;
    public AlwaysEvent(T value) { _value = value; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // It immediately attempts to claim the state. If it succeeds, it bypasses queues entirely.
        if (sharedState.TryClaim())
        {
            sharedState.MarkSynchronized(eventId);
            Scheduler.Enqueue(() => onSync(_value));
        }
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