using System.Threading.Tasks;

namespace Bjoml;

public static class ChannelExtensions
{
    public static Task<T> GetMessage<T>(this Channel<T> channel)
    {
        return Cml.SyncAsync(new ChannelReceiveEvent<T>(channel));
    }

    public static Task PutMessage<T>(this Channel<T> channel, T value)
    {
        return Cml.SyncAsync(new ChannelSendEvent<T>(channel, value));
    }
}
