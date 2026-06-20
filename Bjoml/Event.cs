namespace Bjoml;

public interface IEvent
{
    void Publish(SyncState sharedState);
}

public static class Cml
{
    public static void Sync(params IEvent[] events)
    {
        var sharedState = new SyncState();
        for (int i = 0; i < events.Length; i++)
        {
            events[i].Publish(sharedState);
        }
    }
}

public class ChannelSendEvent<T> : IEvent
{
    private readonly Channel<T> _channel;
    private readonly T _value;
    private readonly Action _onSent;

    public ChannelSendEvent(Channel<T> channel, T value, Action onSent)
    {
        _channel = channel;
        _value = value;
        _onSent = onSent;
    }

    public void Publish(SyncState sharedState)
    {
        _channel.PublishSend(sharedState, _value, _onSent);
    }
}

public class ChannelReceiveEvent<T> : IEvent
{
    private readonly Channel<T> _channel;
    private readonly Action<T> _onReceived;

    public ChannelReceiveEvent(Channel<T> channel, Action<T> onReceived)
    {
        _channel = channel;
        _onReceived = onReceived;
    }

    public void Publish(SyncState sharedState)
    {
        _channel.PublishReceive(sharedState, _onReceived);
    }
}