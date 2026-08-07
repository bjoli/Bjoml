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
using System.Runtime.CompilerServices;
using System.Threading;

namespace Bjoml;

// ---------------------------------------------------------------------------
// The boxed state machine. This is where the dynamic-context shim lives.
// ---------------------------------------------------------------------------

/// <summary>
/// Heap home for a fiber's state machine, allocated once at the first suspension
/// (the same trick the BCL's <c>AsyncTaskMethodBuilder</c> uses).
///
/// After the first box, the compiler keeps calling <c>MoveNext</c> on THIS copy, so
/// later <c>AwaitOnCompleted</c> calls hand us a reference to this very field and we
/// must not copy the state machine again.
///
/// It implements <see cref="IThreadPoolWorkItem"/> directly, so resuming a fiber
/// from the scheduler costs no wrapper allocation at all.
/// </summary>
internal sealed class FiberStateMachineBox<TStateMachine> : IThreadPoolWorkItem
    where TStateMachine : IAsyncStateMachine
{
    public TStateMachine StateMachine = default!;

    /// <summary>
    /// The fiber's dynamic environment, re-captured at every suspension so that
    /// changes made between two awaits are carried forward.
    /// </summary>
    public object? Context;

    private readonly Action _moveNext;
    public Action MoveNextAction => _moveNext;

    public FiberStateMachineBox() => _moveNext = Run;

    /// <summary>
    /// Resume the fiber with its own dynamic environment installed.
    ///
    /// Save-and-restore rather than plain assignment: continuations run inline on
    /// whichever thread completed the rendezvous, and that thread may be several
    /// frames deep inside a DIFFERENT fiber. We are borrowing it, so we hand it back
    /// exactly as we found it.
    ///
    /// Note we do NOT touch ExecutionContext. That is the whole point: the hosted
    /// language keeps its dynamic state here, and reinstating C#'s ambient context
    /// on top would be both wasted work and wrong.
    /// </summary>
    private void Run()
    {
        var prev = FiberContext.Current;
        FiberContext.Current = Context;
        try
        {
            StateMachine.MoveNext();
        }
        finally
        {
            FiberContext.Current = prev;
        }
    }

    /// <summary>Scheduler entry point when the resume was queued rather than inline.</summary>
    public void Execute()
    {
        Scheduler.InlineDepth = 0;
        try
        {
            Run();
        }
        catch (Exception ex)
        {
            // MoveNext normally routes exceptions to SetException; reaching here
            // means the state machine itself failed, which must not kill the process.
            Scheduler.ReportUnhandled(ex);
        }
    }
}

// ---------------------------------------------------------------------------
// Core: the shared mutable half of a fiber. The builder is a struct and gets
// copied around; this reference is what actually persists.
// ---------------------------------------------------------------------------

public sealed class FiberCore<T> : Promise<T>
{
    private object? _box;

    public void SetResult(T value) => TrySetResult(value);
    public void SetException(Exception e) => TrySetException(e);

    /// <summary>
    /// Get the delegate that resumes this fiber, boxing the state machine on first
    /// use and re-capturing the dynamic context on every subsequent suspension.
    /// </summary>
    internal Action GetMoveNextAction<TStateMachine>(ref TStateMachine stateMachine)
        where TStateMachine : IAsyncStateMachine
    {
        if (_box is FiberStateMachineBox<TStateMachine> existing)
        {
            // Re-capture: the fiber may have changed its own context since the last
            // suspension, e.g. by entering a (parameterize ...).
            existing.Context = FiberContext.Current;
            return existing.MoveNextAction;
        }

        var box = new FiberStateMachineBox<TStateMachine>
        {
            StateMachine = stateMachine,   // the one and only copy from the stack
            Context = FiberContext.Current,
        };
        _box = box;
        return box.MoveNextAction;
    }

    /// <summary>The boxed resume, usable as a work item with no wrapper allocation.</summary>
    internal IThreadPoolWorkItem? WorkItem => _box as IThreadPoolWorkItem;
}

// ---------------------------------------------------------------------------
// The task-like types
// ---------------------------------------------------------------------------

/// <summary>
/// Return type of a compiled bjoroutine that yields a value.
///
/// Deliberately NOT a first-class language value: it is a compiler artifact, in the
/// same way <c>Task</c> is for C#. What escapes into the language is
/// <see cref="Promise{T}"/>, which unlike a fiber (or a task) is composable with
/// <c>choose</c>.
/// </summary>
[AsyncMethodBuilder(typeof(FiberMethodBuilder<>))]
public readonly struct Fiber<T>
{
    internal readonly FiberCore<T> Core;
    internal Fiber(FiberCore<T> core) => Core = core;

    /// <summary>The first-class handle. This is what <c>spawn</c> hands to the language.</summary>
    public Promise<T> AsPromise() => Core;

    public PromiseAwaiter<T> GetAwaiter() => Core.GetAwaiter();
}

/// <summary>Return type of a compiled bjoroutine with no useful value.</summary>
[AsyncMethodBuilder(typeof(FiberMethodBuilder))]
public readonly struct Fiber
{
    internal readonly FiberCore<Unit> Core;
    internal Fiber(FiberCore<Unit> core) => Core = core;

    public Promise<Unit> AsPromise() => Core;

    public PromiseAwaiter<Unit> GetAwaiter() => Core.GetAwaiter();
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

public struct FiberMethodBuilder<T>
{
    private FiberCore<T> _core;

    public static FiberMethodBuilder<T> Create()
    {
        var b = default(FiberMethodBuilder<T>);
        b._core = new FiberCore<T>();
        return b;
    }

    // The compiler looks for a property literally named `Task`.
    public Fiber<T> Task => new Fiber<T>(_core);

    public void Start<TStateMachine>(ref TStateMachine stateMachine)
        where TStateMachine : IAsyncStateMachine
    {
        // No ExecutionContext capture or restore. The body runs on the caller's
        // thread with the caller's dynamic environment already installed, which is
        // exactly what a direct procedure call should look like.
        stateMachine.MoveNext();
    }

    public void SetStateMachine(IAsyncStateMachine stateMachine) { /* legacy/debugger only */ }

    public void SetResult(T result) => _core.SetResult(result);

    public void SetException(Exception exception) => _core.SetException(exception);

    /// <summary>
    /// Routed to the unsafe path on purpose: BjoML never flows ExecutionContext.
    /// </summary>
    public void AwaitOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => awaiter.OnCompleted(_core.GetMoveNextAction(ref stateMachine));

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => awaiter.UnsafeOnCompleted(_core.GetMoveNextAction(ref stateMachine));
}

public struct FiberMethodBuilder
{
    private FiberCore<Unit> _core;

    public static FiberMethodBuilder Create()
    {
        var b = default(FiberMethodBuilder);
        b._core = new FiberCore<Unit>();
        return b;
    }

    public Fiber Task => new Fiber(_core);

    public void Start<TStateMachine>(ref TStateMachine stateMachine)
        where TStateMachine : IAsyncStateMachine
        => stateMachine.MoveNext();

    public void SetStateMachine(IAsyncStateMachine stateMachine) { }

    public void SetResult() => _core.SetResult(default);

    public void SetException(Exception exception) => _core.SetException(exception);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => awaiter.OnCompleted(_core.GetMoveNextAction(ref stateMachine));

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => awaiter.UnsafeOnCompleted(_core.GetMoveNextAction(ref stateMachine));
}
