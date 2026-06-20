using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public abstract class Operation
{
    public SyncState State; 

    public bool IsSynchronized => State.IsSynchronized;
    public bool TryClaim() => State.TryClaim();
    public bool TrySync() => State.TrySync();
    public void MarkSynchronized() => State.MarkSynchronized();
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
        return true;
    }
}