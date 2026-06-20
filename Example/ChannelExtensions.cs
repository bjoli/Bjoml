using System.Threading.Tasks;

namespace Bjoml;

public static class ChannelExtensions
{
    public static Task<T> GetMessage<T>(this Channel<T> channel)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.PublishReceive(new SyncState(), value => tcs.SetResult(value));
        return tcs.Task;
    }

    public static Task PutMessage<T>(this Channel<T> channel, T value)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.PublishSend(new SyncState(), value, () => tcs.SetResult());
        return tcs.Task;
    }
}
