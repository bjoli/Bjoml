using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public abstract class Operation
{
    public SyncState State; 
    public int EventId;

    public bool IsSynchronized => State.IsSynchronized;
    public bool TryClaim() => State.TryClaim();
    public bool TrySync() => State.TrySync();
    public void MarkSynchronized() => State.MarkSynchronized(EventId);
    public void ResetClaim() => State.ResetClaim();
}

public class PutOp<T> : Operation, IResettable
{
    public T Value;
    public Action ResumePut;

    public bool TryReset()
    {
        Value = default!;
        ResumePut = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}

public class GetOp<T> : Operation, IResettable
{
    public Action<T> ResumeGet;

    public bool TryReset()
    {
        ResumeGet = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}