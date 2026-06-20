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

public class ChooseEvent<T> : IEvent<T>
{
    private readonly IEvent<T>[] _events;
    public ChooseEvent(params IEvent<T>[] events) { _events = events; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
        // For choose, we generate a new ID for each branch, so we know which one won
        foreach (var ev in _events)
        {
            ev.Publish(sharedState, sharedState.GenerateEventId(), onSync);
        }
    }
}

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
        _ev.Publish(sharedState, eventId, value => onSync(_mapper(value)));
    }
}

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
        // Register the nack so that if this specific eventId DOES NOT WIN, we fire the nack channel
        sharedState.RegisterNack(eventId, () => nackChan.PublishSend(new SyncState(), 1, new Unit(), () => { }));

        var ev = _generator(new ChannelReceiveEvent<Unit>(nackChan));
        
        // Publish the generated event WITH OUR EVENT ID. 
        // This means if `ev` wins, it uses our eventId as the winner, 
        // so our nack is NOT fired!
        ev.Publish(sharedState, eventId, onSync);
    }
}

public class AlwaysEvent<T> : IEvent<T>
{
    private readonly T _value;
    public AlwaysEvent(T value) { _value = value; }

    public void Publish(SyncState sharedState, int eventId, Action<T> onSync)
    {
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
        // Do nothing. It never synchronizes.
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