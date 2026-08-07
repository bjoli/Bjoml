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
using System.Threading;
using System.Threading.Tasks;

namespace Bjoml;

public static class Bjo
{
    // -----------------------------------------------------------------------
    // spawn
    // -----------------------------------------------------------------------

    /// <summary>
    /// <c>(spawn bjoroutine)</c>. Returns a <see cref="Promise{T}"/>, NOT a
    /// <see cref="Fiber{T}"/>: a fiber is a compiler artifact, while a promise is a
    /// first-class value that is both joinable and composable with <c>choose</c>.
    /// That composability is the whole point — you cannot write
    /// <c>(sync (choose (promise-join p) (channel-get cancel)))</c> if spawn hands
    /// back a task.
    ///
    /// The body starts on the pool rather than on the caller's stack, so spawn is
    /// genuinely concurrent instead of "run until the first suspension".
    ///
    /// The child INHERITS the spawning fiber's dynamic environment.
    ///
    /// Note for callers: C# cannot infer <typeparamref name="T"/> from an async
    /// lambda, so write <c>Bjo.Spawn&lt;int&gt;(async () =&gt; ...)</c>.
    /// </summary>
    public static Promise<T> Spawn<T>(Func<Fiber<T>> body)
    {
        var inherited = FiberContext.Current;
        var core = new FiberCore<T>(SpawnRunners<T>.FuncRunner, body, null, inherited);
        Scheduler.Enqueue(core);
        return core;
    }

    /// <summary>
    /// Spawn a bjoroutine with state and a result. Enables zero-closure static lambdas.
    /// </summary>
    public static Promise<TResult> Spawn<TState, TResult>(Func<TState, Fiber<TResult>> body, TState state)
    {
        var inherited = FiberContext.Current;
        var core = new FiberCore<TResult>(SpawnStateRunners<TState, TResult>.StateRunner, body, state, inherited);
        Scheduler.Enqueue(core);
        return core;
    }

    /// <summary>Spawn a bjoroutine with no useful result.</summary>
    public static Promise<Unit> Spawn(Func<Fiber> body)
    {
        var inherited = FiberContext.Current;
        var core = new FiberCore<Unit>(SpawnUnitRunners.FuncRunner, body, null, inherited);
        Scheduler.Enqueue(core);
        return core;
    }

    /// <summary>
    /// Spawn a bjoroutine with state and no useful result. Enables zero-closure static lambdas.
    /// </summary>
    public static Promise<Unit> Spawn<TState>(Func<TState, Fiber> body, TState state)
    {
        var inherited = FiberContext.Current;
        var core = new FiberCore<Unit>(SpawnStateRunners<TState, Unit>.UnitStateRunner, body, state, inherited);
        Scheduler.Enqueue(core);
        return core;
    }

    // -----------------------------------------------------------------------
    // entry point
    // -----------------------------------------------------------------------

    /// <summary>
    /// Run a bjoroutine on the current thread and block until it finishes. For the
    /// program entry point.
    ///
    /// DO NOT call this from a thread-pool thread: the fiber needs pool threads to
    /// make progress and you are holding one hostage.
    /// </summary>
    public static T RunToCompletion<T>(Func<Fiber<T>> body)
    {
        var p = body().AsPromise();
        WaitFor(p);
        return p.GetAwaiter().GetResult();
    }

    public static void RunToCompletion(Func<Fiber> body)
    {
        var p = body().AsPromise();
        WaitFor(p);
        p.GetAwaiter().GetResult();
    }

    /// <summary>
    /// Block until a promise lands.
    ///
    /// Uses a monitor rather than a ManualResetEventSlim because the completing
    /// thread must not be able to touch a disposed handle after we wake.
    /// </summary>
    private static void WaitFor<T>(Promise<T> p)
    {
        var gate = new object();
        bool completed = false;

        p.GetAwaiter().OnCompleted(() =>
        {
            lock (gate)
            {
                completed = true;
                Monitor.Pulse(gate);
            }
        });

        lock (gate)
        {
            while (!completed) Monitor.Wait(gate);
        }
    }
}

internal static class SpawnRunners<T>
{
    public static readonly Action<FiberCore<T>> FuncRunner = static core =>
    {
        var func = (Func<Fiber<T>>)core._spawnBody!;
        var fiber = func();
        if (!ReferenceEquals(fiber.Core, core)) fiber.AsPromise().Forward(core);
    };
}

internal static class SpawnUnitRunners
{
    public static readonly Action<FiberCore<Unit>> FuncRunner = static core =>
    {
        var func = (Func<Fiber>)core._spawnBody!;
        var fiber = func();
        if (!ReferenceEquals(fiber.Core, core)) fiber.AsPromise().Forward(core);
    };
}

internal static class SpawnStateRunners<TState, TResult>
{
    public static readonly Action<FiberCore<TResult>> StateRunner = static core =>
    {
        var func = (Func<TState, Fiber<TResult>>)core._spawnBody!;
        var state = (TState)core._spawnState!;
        var fiber = func(state);
        if (!ReferenceEquals(fiber.Core, core)) fiber.AsPromise().Forward(core);
    };

    public static readonly Action<FiberCore<Unit>> UnitStateRunner = static core =>
    {
        var func = (Func<TState, Fiber>)core._spawnBody!;
        var state = (TState)core._spawnState!;
        var fiber = func(state);
        if (!ReferenceEquals(fiber.Core, core)) fiber.AsPromise().Forward(core);
    };
}

public static class TaskInterop
{
    // -----------------------------------------------------------------------
    // Task -> Promise
    // -----------------------------------------------------------------------

    /// <summary>
    /// Bring a C# task into BjoML.
    ///
    /// The task is ALREADY RUNNING. Losing a <c>choose</c> does not stop it, it only
    /// drops the result: a task cannot be un-started, whereas a channel event that
    /// loses is cleanly withdrawn. Prefer <see cref="Cancellable{T}"/>, which makes
    /// the difference go away.
    /// </summary>
    public static Promise<T> FromTask<T>(Task<T> task)
    {
        var p = new Promise<T>();

        // ConfigureAwait(false) + UnsafeOnCompleted rather than ContinueWith:
        // ContinueWith captures an ExecutionContext and reinstates it around the
        // continuation, which is exactly what BjoML does not want.
        var awaiter = task.ConfigureAwait(false).GetAwaiter();
        if (awaiter.IsCompleted) Settle(task, p);
        else awaiter.UnsafeOnCompleted(() => Settle(task, p));

        return p;
    }

    public static Promise<Unit> FromTask(Task task) => FromTask(Wrap(task));

    private static async Task<Unit> Wrap(Task t)
    {
        await t.ConfigureAwait(false);
        return default;
    }

    private static void Settle<T>(Task<T> t, Promise<T> p)
    {
        if (t.IsCanceled) p.TrySetException(new TaskCanceledException(t));
        else if (t.IsFaulted) p.TrySetException(t.Exception!.GetBaseException());
        else p.TrySetResult(t.Result);
    }

    // -----------------------------------------------------------------------
    // Promise -> Task, for calling back into C#
    // -----------------------------------------------------------------------

    public static Task<T> ToTask<T>(this Promise<T> p)
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        p.GetAwaiter().OnCompleted(() =>
        {
            try { tcs.TrySetResult(p.GetAwaiter().GetResult()); }
            catch (Exception e) { tcs.TrySetException(e); }
        });

        return tcs.Task;
    }

    // -----------------------------------------------------------------------
    // The only form that should be exposed to the language for use in choose
    // -----------------------------------------------------------------------

    /// <summary>
    /// Wrap a cancellable async operation as a proper CML event: if this branch
    /// loses the <c>choose</c>, the nack fires, the token is cancelled and the
    /// underlying work actually stops rather than running on to no purpose.
    ///
    /// Language surface: <c>(task-&gt;event (lambda (cancel-token) ...))</c>. Making
    /// the uncancellable form hard to reach is worth the inconvenience; otherwise
    /// every choose over I/O leaks work.
    ///
    /// Note this is still a PERSISTENT event: once the task has completed, syncing
    /// on it again yields the same value immediately, so it will win every iteration
    /// of a loop and starve its siblings. Same trap as <c>Cml.Always</c>.
    /// </summary>
    public static IEvent<Result<T>> Cancellable<T>(Func<CancellationToken, Task<T>> start)
        => Cml.WithNack<Result<T>>(nack =>
        {
            var cts = new CancellationTokenSource();

            // Dispose exactly once, from whichever of the two paths gets there first.
            int disposed = 0;
            void DisposeOnce()
            {
                if (Interlocked.Exchange(ref disposed, 1) == 0) cts.Dispose();
            }

            // A bare callback rather than a spawned fiber. The proposed version
            // spawned a fiber to await the nack, which meant that whenever this
            // branch WON, that fiber parked forever holding the token source. This
            // runs no user code, so it is safe outside a fiber context.
            Cml.Sync(nack, _ =>
            {
                try { cts.Cancel(); }
                catch (ObjectDisposedException) { /* the task already finished */ }
                DisposeOnce();
            });

            Task<T> task;
            try
            {
                task = start(cts.Token);
            }
            catch (Exception e)
            {
                // A synchronous throw from the starter is a result, not a crash.
                DisposeOnce();
                return Cml.Always(Result<T>.Fail(e));
            }

            var p = FromTask(task);

            // The winning path also has to clean up; the nack will never fire there.
            p.OnCompleted(DisposeOnce);

            return p.Join();
        });
}
