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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Bjoml;

/// <summary>
/// Success or failure as a VALUE.
///
/// Events must never throw: an exception raised inside an event continuation
/// escapes into a channel's matching loop, where it is swallowed by the
/// scheduler's catch-all and the sync block simply never completes. So a failure
/// travels as a <see cref="Result{T}"/> and is only turned back into an exception
/// inside a fiber, where the state machine can catch it.
/// </summary>
public readonly struct Result<T>
{
    public readonly T Value;
    public readonly ExceptionDispatchInfo? Error;

    private Result(T value, ExceptionDispatchInfo? error)
    {
        Value = value;
        Error = error;
    }

    public static Result<T> Ok(T value) => new Result<T>(value, null);
    public static Result<T> Fail(ExceptionDispatchInfo e) => new Result<T>(default!, e);
    public static Result<T> Fail(Exception e) => new Result<T>(default!, ExceptionDispatchInfo.Capture(e));

    public bool IsError => Error != null;

    /// <summary>
    /// Rethrow the failure with its original stack trace, or return the value.
    ///
    /// Only call this from inside a fiber — i.e. from an awaiter's
    /// <c>GetResult()</c>, which runs on the state machine's stack. Calling it from
    /// an event continuation throws into the channel matching loop instead.
    /// </summary>
    public T Unwrap()
    {
        Error?.Throw();
        return Value;
    }
}

/// <summary>
/// Something waiting for a promise to land.
///
/// A waiter can be ABANDONED: a promise branch inside a <c>choose</c> that another
/// branch won is dead, but the promise itself may never complete, so nothing would
/// ever walk the list and drop it. Without an abandonment test, every losing
/// <c>choose</c> over a long-lived promise leaks its whole sync block.
/// </summary>
internal interface IPromiseWaiter
{
    void Signal();
    bool IsAbandoned { get; }
}

/// <summary>
/// A write-once cell that is also a PERSISTENT CML event.
///
/// This is the handle type for <c>spawn</c> and the bridge type for C# tasks.
/// Unlike a channel event it is not consumed by syncing: once completed, every sync
/// on it succeeds immediately with the same value.
///
/// STARVATION WARNING: a completed promise inside a <c>choose</c> loop wins every
/// iteration, exactly like <c>Cml.Always</c>. Document this for language users.
/// </summary>
public sealed class Promise<T> : IEvent<Result<T>>
{
    private const int Pending = 0;
    private const int Completed = 1;

    private readonly object _lock = new();
    private int _state = Pending;
    private T _value = default!;
    private ExceptionDispatchInfo? _error;
    private List<IPromiseWaiter>? _waiters;

    public bool IsCompleted => Volatile.Read(ref _state) == Completed;

    public bool TrySetResult(T value) => Complete(value, null);

    public bool TrySetException(Exception e) => Complete(default!, ExceptionDispatchInfo.Capture(e));

    public bool TrySetException(ExceptionDispatchInfo e) => Complete(default!, e);

    private bool Complete(T value, ExceptionDispatchInfo? error)
    {
        List<IPromiseWaiter>? toRun;
        lock (_lock)
        {
            if (_state == Completed) return false;

            _value = value;
            _error = error;

            // Release: publishes _value/_error to any thread that subsequently reads
            // IsCompleted, which is an acquiring read.
            Volatile.Write(ref _state, Completed);

            toRun = _waiters;
            _waiters = null;
        }

        if (toRun != null)
        {
            foreach (var w in toRun)
            {
                // Enqueue rather than run inline: the completing thread may be a
                // foreign one (a Task continuation, a timer) that we should not
                // borrow for arbitrary fiber code, and inline completion here would
                // let one promise chain grow the stack without bound.
                if (!w.IsAbandoned) Scheduler.Enqueue(w.Signal);
            }
        }

        return true;
    }

    // ---- waiter registration ----------------------------------------------

    internal void Register(IPromiseWaiter waiter)
    {
        lock (_lock)
        {
            if (_state != Completed)
            {
                _waiters ??= new List<IPromiseWaiter>();

                // Drop waiters belonging to sync blocks that have since been won by
                // another branch. This is what bounds the list on a long-lived
                // promise that is repeatedly offered in a choose and repeatedly loses.
                if (_waiters.Count >= 8)
                    _waiters.RemoveAll(static w => w.IsAbandoned);

                _waiters.Add(waiter);
                return;
            }
        }

        if (!waiter.IsAbandoned) waiter.Signal();
    }

    /// <summary>Run <paramref name="k"/> now if already complete, else on completion.</summary>
    internal void OnCompleted(Action k) => Register(new ActionWaiter(k));

    private sealed class ActionWaiter : IPromiseWaiter
    {
        private readonly Action _k;
        public ActionWaiter(Action k) => _k = k;
        public void Signal() => _k();

        // A fiber awaiting a promise directly is never abandoned; it has nothing
        // else it could be doing.
        public bool IsAbandoned => false;
    }

    internal Result<T> Outcome
    {
        get
        {
            // Callers must have observed IsCompleted first; that acquiring read
            // pairs with the release write in Complete.
            return _error != null ? Result<T>.Fail(_error) : Result<T>.Ok(_value);
        }
    }

    /// <summary>Pipe this promise's outcome into <paramref name="target"/> when it lands.</summary>
    internal void Forward(Promise<T> target)
        => OnCompleted(() =>
        {
            var r = Outcome;
            if (r.IsError) target.TrySetException(r.Error!);
            else target.TrySetResult(r.Value);
        });

    // ---- CML surface -------------------------------------------------------

    /// <summary>
    /// The joinable event. Carries failure as a value; unwrap it inside a fiber.
    /// Returns this instance directly to avoid allocating event wrapper objects.
    /// </summary>
    public IEvent<Result<T>> Join() => this;

    public void Publish(SyncState state, int eventId, Action<Result<T>> onSync)
    {
        if (IsCompleted)
        {
            Deliver(state, eventId, onSync);
            return;
        }

        Register(new Waiter(this, state, eventId, onSync));
    }

    private void Deliver(SyncState state, int eventId, Action<Result<T>> onSync)
    {
        // TryCommit, not TryClaim. This waiter can run on a completely different
        // thread long after Publish returned, and may well find the state transiently
        // Claimed by a sibling branch still being published. Treating that as "someone
        // else won" would drop a completion that actually happened, and the choose
        // would then wait forever on an event that already fired.
        if (!state.TryCommit(eventId)) return;

        Scheduler.Dispatch(onSync, Outcome);
    }

    private sealed class Waiter : IPromiseWaiter
    {
        private readonly Promise<T> _owner;
        private readonly SyncState _state;
        private readonly int _eventId;
        private readonly Action<Result<T>> _onSync;

        public Waiter(Promise<T> owner, SyncState state, int eventId, Action<Result<T>> onSync)
        {
            _owner = owner;
            _state = state;
            _eventId = eventId;
            _onSync = onSync;
        }

        public void Signal() => _owner.Deliver(_state, _eventId, _onSync);

        /// <summary>Our sync block was won by another branch; we can be dropped.</summary>
        public bool IsAbandoned => _state.IsSynchronized;
    }

    // ---- direct-await surface (cheaper than routing through Cml.Sync) ------

    public PromiseAwaiter<T> GetAwaiter() => new PromiseAwaiter<T>(this);
}

public readonly struct PromiseAwaiter<T> : ICriticalNotifyCompletion
{
    private readonly Promise<T> _p;
    public PromiseAwaiter(Promise<T> p) => _p = p;

    public bool IsCompleted => _p.IsCompleted;

    // Called from inside MoveNext, so throwing here is converted to SetException by
    // the state machine. This is the correct place for a failure to surface.
    public T GetResult() => _p.Outcome.Unwrap();

    public void OnCompleted(Action continuation) => _p.OnCompleted(continuation);

    // No ExecutionContext capture, by design. See FiberContext.
    public void UnsafeOnCompleted(Action continuation) => _p.OnCompleted(continuation);
}
