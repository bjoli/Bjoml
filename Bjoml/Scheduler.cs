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

namespace Bjoml;

using System;
using System.Threading;
using System.Runtime.CompilerServices;

/// <summary>
/// Work dispatch for BjoML.
///
/// BjoML runs on the .NET thread pool rather than on dedicated worker threads.
/// The pool already implements per-thread local queues with work stealing, which is
/// exactly what the old <c>BlockingCollection</c>-per-worker design lacked: a fiber
/// that spawned 10 000 children pinned all of them to its own queue while every
/// other worker sat idle in <c>GetConsumingEnumerable()</c>.
///
/// Every enqueue goes through <see cref="ThreadPool.UnsafeQueueUserWorkItem(IThreadPoolWorkItem, bool)"/>
/// with <c>preferLocal: true</c>, so work produced by a rendezvous lands on the
/// producing thread's local queue and is picked up without a global handoff, while
/// remaining stealable when that thread falls behind.
///
/// EXECUTION CONTEXT: nothing here ever captures or restores an
/// <see cref="ExecutionContext"/>. That is deliberate and load-bearing. The hosted
/// language carries its own dynamic environment (see <see cref="FiberContext"/>),
/// so flowing EC would be pure overhead *and* would reinstate ambient C# state on
/// top of the fiber's own. "Unsafe" in <c>UnsafeQueueUserWorkItem</c> means
/// precisely "do not flow EC", which is what we want on every path.
/// </summary>
public static class Scheduler
{
    /// <summary>
    /// Maximum number of nested inline dispatches before we bounce to the pool.
    ///
    /// This is the only thing standing between a long chain of rendezvous
    /// continuations and a stack overflow, since continuations run inline on the
    /// thread that completed the match. Treat it as load-bearing.
    /// </summary>
    public const int MaxInlineDepth = 50;

    [ThreadStatic]
    internal static int InlineDepth;

    /// <summary>
    /// Where exceptions escaping a scheduled work item go. Defaults to stderr.
    ///
    /// Unlike the old dedicated workers, an unhandled exception on a thread-pool
    /// thread tears down the process, so every work item body is wrapped.
    /// </summary>
    public static Action<Exception> UnhandledException { get; set; } =
        static ex => Console.Error.WriteLine($"BjoML unhandled exception: {ex}");

    /// <summary>
    /// Optional tuning hook, kept for source compatibility with the old
    /// dedicated-worker API. There are no workers to start any more; this only
    /// raises the pool's minimum thread count so a burst of fibers does not wait
    /// on the pool's thread-injection heuristic.
    /// </summary>
    public static void Start(int numWorkers = 0)
    {
        if (numWorkers <= 0) numWorkers = Environment.ProcessorCount;

        ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);
        if (workerThreads < numWorkers)
            ThreadPool.SetMinThreads(numWorkers, completionPortThreads);
    }

    // ---- enqueue -----------------------------------------------------------

    /// <summary>
    /// Queue a work item. Prefer this overload: a caller that can implement
    /// <see cref="IThreadPoolWorkItem"/> on an object it already owns (as
    /// <c>FiberStateMachineBox</c> does) enqueues with zero allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Enqueue(IThreadPoolWorkItem item)
        => ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: true);

    public static void Enqueue(Action work)
        => ThreadPool.UnsafeQueueUserWorkItem(ActionWorkItem.Rent(work), preferLocal: true);

    public static void Enqueue<T>(Action<T> work, T state)
        => ThreadPool.UnsafeQueueUserWorkItem(ActionWorkItem<T>.Rent(work, state), preferLocal: true);

    // ---- dispatch ----------------------------------------------------------

    /// <summary>
    /// Run a continuation now if the inline budget allows, otherwise queue it.
    ///
    /// Running inline is the fast path and the common case: the thread that
    /// completed a rendezvous simply keeps going into the continuation instead of
    /// paying a queue round trip.
    /// </summary>
    public static void Dispatch(Action action)
    {
        int depth = InlineDepth;
        if (depth < MaxInlineDepth)
        {
            InlineDepth = depth + 1;
            try { action(); }
            finally { InlineDepth = depth; }
        }
        else
        {
            Enqueue(action);
        }
    }

    public static void Dispatch<T>(Action<T> action, T state)
    {
        int depth = InlineDepth;
        if (depth < MaxInlineDepth)
        {
            InlineDepth = depth + 1;
            try { action(state); }
            finally { InlineDepth = depth; }
        }
        else
        {
            Enqueue(action, state);
        }
    }

    internal static void ReportUnhandled(Exception ex)
    {
        try { UnhandledException(ex); }
        catch { /* a throwing handler must not take the process down */ }
    }
}

/// <summary>
/// Pooled <see cref="IThreadPoolWorkItem"/> wrapper for a bare <see cref="Action"/>.
///
/// The free list is <c>[ThreadStatic]</c> and therefore needs no synchronisation at
/// all. Items are rented on the producing thread and recycled on the consuming
/// thread, so a given thread's list drifts in length; that is harmless, it is only
/// a cache, and both ends fall back to allocation.
/// </summary>
internal sealed class ActionWorkItem : IThreadPoolWorkItem
{
    private const int MaxCached = 32;

    [ThreadStatic] private static ActionWorkItem? _free;
    [ThreadStatic] private static int _freeCount;

    private ActionWorkItem? _next;
    private Action _action = null!;

    public static ActionWorkItem Rent(Action action)
    {
        var item = _free;
        if (item is null) return new ActionWorkItem { _action = action };

        _free = item._next;
        _freeCount--;
        item._next = null;
        item._action = action;
        return item;
    }

    public void Execute()
    {
        var action = _action;

        // Clear and recycle BEFORE running the body: the body may not return
        // normally, and we must never touch our own fields once we are back on a
        // free list.
        _action = null!;
        if (_freeCount < MaxCached)
        {
            _next = _free;
            _free = this;
            _freeCount++;
        }

        // A pool thread is reused across work items, and a previous item may have
        // unwound mid-dispatch, so the inline budget is reset per item.
        Scheduler.InlineDepth = 0;
        try
        {
            action();
        }
        catch (Exception ex)
        {
            Scheduler.ReportUnhandled(ex);
        }
    }
}

/// <summary>
/// Pooled <see cref="IThreadPoolWorkItem"/> wrapper for an <see cref="Action{T}"/>
/// plus its state, so a continuation carrying a value needs no closure allocation.
/// </summary>
internal sealed class ActionWorkItem<T> : IThreadPoolWorkItem
{
    private const int MaxCached = 32;

    [ThreadStatic] private static ActionWorkItem<T>? _free;
    [ThreadStatic] private static int _freeCount;

    private ActionWorkItem<T>? _next;
    private Action<T> _action = null!;
    private T _state = default!;

    public static ActionWorkItem<T> Rent(Action<T> action, T state)
    {
        var item = _free;
        if (item is null) return new ActionWorkItem<T> { _action = action, _state = state };

        _free = item._next;
        _freeCount--;
        item._next = null;
        item._action = action;
        item._state = state;
        return item;
    }

    public void Execute()
    {
        var action = _action;
        var state = _state;

        _action = null!;
        _state = default!;   // drop the reference so a cached item never pins a value
        if (_freeCount < MaxCached)
        {
            _next = _free;
            _free = this;
            _freeCount++;
        }

        Scheduler.InlineDepth = 0;
        try
        {
            action(state);
        }
        catch (Exception ex)
        {
            Scheduler.ReportUnhandled(ex);
        }
    }
}
