using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public class Channel<T>
{
    internal readonly ConcurrentQueue<PutOp<T>> _putq = new();
    internal readonly ConcurrentQueue<GetOp<T>> _getq = new();

    internal readonly ObjectPool<PutOp<T>> _putPool = ObjectPool.Create<PutOp<T>>();
    internal readonly ObjectPool<GetOp<T>> _getPool = ObjectPool.Create<GetOp<T>>();

    public void PublishSend(SyncState state, T value, Action resumePut)
    {
        var myOp = _putPool.Get();
        myOp.State = state;
        myOp.Value = value;
        myOp.ResumePut = resumePut;

        _putq.Enqueue(myOp);

        while (true)
        {
            if (!state.TryClaim()) 
            {
                return;
            }

            if (_getq.TryDequeue(out var getOp))
            {
                if (getOp.IsSynchronized) 
                {
                    _getPool.Return(getOp);
                    state.ResetClaim();
                    continue;
                }

                if (getOp.TrySync())
                {
                    var getResume = getOp.ResumeGet;
                    state.MarkSynchronized();

                    var myResume = resumePut;
                    T capturedValue = value;

                    Scheduler.Enqueue(() => myResume());
                    Scheduler.Enqueue(() => getResume(capturedValue));
                    
                    _getPool.Return(getOp);
                    return;
                }
                else
                {
                    _getPool.Return(getOp);
                    state.ResetClaim();
                }
            }
            else
            {
                state.ResetClaim();
                return;
            }
        }
    }

    public void PublishReceive(SyncState state, Action<T> resumeGet)
    {
        var myOp = _getPool.Get();
        myOp.State = state;
        myOp.ResumeGet = resumeGet;

        _getq.Enqueue(myOp);

        while (true)
        {
            if (!state.TryClaim()) 
            {
                return;
            }

            if (_putq.TryDequeue(out var putOp))
            {
                if (putOp.IsSynchronized) 
                {
                    _putPool.Return(putOp);
                    state.ResetClaim();
                    continue;
                }

                if (putOp.TrySync())
                {
                    T capturedValue = putOp.Value;
                    var putResume = putOp.ResumePut;

                    state.MarkSynchronized();

                    var myResume = resumeGet;

                    Scheduler.Enqueue(() => putResume());
                    Scheduler.Enqueue(() => myResume(capturedValue));

                    _putPool.Return(putOp);
                    return;
                }
                else
                {
                    _putPool.Return(putOp);
                    state.ResetClaim();
                }
            }
            else
            {
                state.ResetClaim();
                return;
            }
        }
    }
}