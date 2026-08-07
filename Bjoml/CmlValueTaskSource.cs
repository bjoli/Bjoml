// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
//
// This file is part of BjoML.
//
// BjoML is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// BjoML is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with BjoML.  If not, see <https://www.gnu.org/licenses/>.

using System;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.ObjectPool;

namespace Bjoml;

/// <summary>
/// Pooled <see cref="IValueTaskSource{T}"/> backing the plain-C# <c>ValueTask</c>
/// surface (<see cref="ChannelExtensions"/>, <see cref="SimpleChannel{T}"/>).
///
/// This is the interop path, not the fiber path. Fibers await
/// <see cref="EventAwaiter{T}"/> directly and never allocate one of these.
///
/// KNOWN LEAK: <see cref="GetResult"/> is what resets the core and returns the
/// source to the pool, so a <c>ValueTask</c> that is created and never awaited is
/// never recycled and keeps its continuation and result alive. In the hosted
/// language, <c>(put! ch v)</c> in statement position looks exactly like a discard,
/// so the compiler must always await it — or, better, use the event/awaiter path,
/// which has no pooled source at all.
///
/// <c>_core.RunContinuationsAsynchronously</c> is deliberately left false, so a
/// continuation runs inline inside <c>SetResult</c>, inside <c>Scheduler.Dispatch</c>,
/// inside the channel matching loop. That is what we want for latency, but it means
/// <see cref="Scheduler.MaxInlineDepth"/> is the only bound on stack growth.
/// </summary>
internal sealed class CmlValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
{
    private static readonly ObjectPool<CmlValueTaskSource<T>> _pool = ObjectPool.Create<CmlValueTaskSource<T>>();
    
    private ManualResetValueTaskSourceCore<T> _core; // mutable struct, must not be readonly
    private readonly Action<T> _onSyncDelegate;
    private readonly Action _onSyncVoidDelegate;

    public CmlValueTaskSource()
    {
        _onSyncDelegate = OnSync;
        _onSyncVoidDelegate = () => OnSync(default!);
    }

    public static CmlValueTaskSource<T> Rent() => _pool.Get();

    public short Version => _core.Version;
    
    public Action<T> OnSyncDelegate => _onSyncDelegate;
    public Action OnSyncVoidDelegate => _onSyncVoidDelegate;

    private void OnSync(T value)
    {
        _core.SetResult(value);
    }

    public T GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            _core.Reset();
            _pool.Return(this);
        }
    }

    void IValueTaskSource.GetResult(short token)
    {
        GetResult(token);
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _core.OnCompleted(continuation, state, token, flags);
    }
}
