using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public abstract class Operation
{
    // Every queued operation holds a reference to the SyncState of the 'Cml.Sync' block that created it.
    // By locking and mutating this shared state, we pair operations atomically.
    public SyncState State; 
    
    // The specific EventId for this branch in a 'Choose' block. 
    // This allows the SyncState to know which branch won, so it can fire the NACKs for the losers.
    public int EventId;

    public bool IsSynchronized => State.IsSynchronized;
    public bool TryClaim() => State.TryClaim();
    public bool TrySync() => State.TrySync();
    public void MarkSynchronized() => State.MarkSynchronized(EventId);
    public void ResetClaim() => State.ResetClaim();
}

/// <summary>
/// Represents a pending 'Put' (Send) operation sitting in a channel's queue.
/// We implement IResettable so these objects can be reused by the ObjectPool, 
/// avoiding garbage collection overhead during millions of channel communications.
/// </summary>
public class PutOp<T> : Operation, IResettable
{
    public T Value;
    public Action ResumePut;

    public bool TryReset()
    {
        // Clear references so they can be garbage collected. 
        // This is crucial to avoid memory leaks of the captured values or continuations while the object sits in the pool.
        Value = default!;
        ResumePut = null!;
        State = null!; 
        EventId = 0;
        return true;
    }
}

/// <summary>
/// Represents a pending 'Get' (Receive) operation sitting in a channel's queue.
/// </summary>
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