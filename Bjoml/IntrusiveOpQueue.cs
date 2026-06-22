using System.Threading;
using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

public class IntrusiveOpQueue<TOp> where TOp : Operation
{
    private TOp? _head;
    private TOp? _tail;
    private readonly object _syncRoot = new object();

    public IntrusiveOpQueue(ObjectPool<TOp> pool)
    {
        // No dummy node needed when using a lock
    }

    public void Enqueue(TOp node)
    {
        node.Next = null;
        lock (_syncRoot)
        {
            if (_tail == null)
            {
                _head = node;
                _tail = node;
            }
            else
            {
                _tail.Next = node;
                _tail = node;
            }
        }
    }

    public bool TryDequeue(out TOp result)
    {
        lock (_syncRoot)
        {
            var h = _head;
            if (h == null)
            {
                result = null!;
                return false;
            }
            
            result = h;
            _head = (TOp?)h.Next;
            
            if (_head == null)
            {
                _tail = null;
            }
            
            result.Next = null;
            return true;
        }
    }
}
