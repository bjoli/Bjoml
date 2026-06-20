using System;
using System.Collections.Concurrent;

namespace Bjoml;

public class Channel<T>
{
    internal readonly ConcurrentQueue<PutOp<T>> _putq = new();
    internal readonly ConcurrentQueue<GetOp<T>> _getq = new();

    public void PublishSend(SyncState state, T value, Action resumePut)
    {
        var myOp = new PutOp<T> { State = state, Value = value, ResumePut = resumePut };

        _putq.Enqueue(myOp);

        while (_getq.TryDequeue(out var getOp))
        {
            if (getOp.IsSynchronized) 
            {
                continue;
            }

            if (myOp.TryClaim())
            {
                if (getOp.TrySync())
                {
                    myOp.MarkSynchronized();

                    T capturedValue = value;
                    var myResume = myOp.ResumePut;
                    var getResume = getOp.ResumeGet;

                    Scheduler.Enqueue(() => myResume());
                    Scheduler.Enqueue(() => getResume(capturedValue));
                    
                    return;
                }
                else
                {
                    myOp.ResetClaim();
                }
            }
            else
            {
                return;
            }
        }
    }

    public void PublishReceive(SyncState state, Action<T> resumeGet)
    {
        var myOp = new GetOp<T> { State = state, ResumeGet = resumeGet };

        _getq.Enqueue(myOp);

        while (_putq.TryDequeue(out var putOp))
        {
            if (putOp.IsSynchronized) 
            {
                continue;
            }

            if (myOp.TryClaim())
            {
                if (putOp.TrySync())
                {
                    myOp.MarkSynchronized();

                    T capturedValue = putOp.Value;
                    var myResume = myOp.ResumeGet;
                    var putResume = putOp.ResumePut;

                    Scheduler.Enqueue(() => putResume());
                    Scheduler.Enqueue(() => myResume(capturedValue));

                    return;
                }
                else
                {
                    myOp.ResetClaim();
                }
            }
            else
            {
                return;
            }
        }
    }
}