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

/// <summary>
/// Makes <c>IEvent&lt;T&gt;</c> awaitable, so <c>(sync ev)</c> in the hosted language
/// compiles to <c>await ev</c> and the suspension points of the language are exactly
/// its sync points. That equivalence is what makes CML reasoning work: between two
/// syncs a fiber is atomic with respect to every other fiber.
/// </summary>
public static class EventAwaitExtensions
{
    public static EventAwaiter<T> GetAwaiter<T>(this IEvent<T> ev) => new EventAwaiter<T>(ev);
}

/// <summary>
/// The synchronisation is STARTED in the constructor, i.e. when <c>await</c>
/// evaluates its operand.
///
/// If the rendezvous matches inline — the common case, because
/// <see cref="Scheduler.Dispatch"/> runs continuations on the matching thread —
/// <see cref="IsCompleted"/> is already true and the compiler never asks for a
/// continuation, so no resume delegate is created and the fiber never suspends.
/// </summary>
public sealed class EventAwaiter<T> : ICriticalNotifyCompletion
{
    /// <summary>Marks "already completed" so a late continuation runs immediately.</summary>
    private static readonly Action Sentinel = () => { };

    private T _result = default!;
    private Action? _continuation;

    public EventAwaiter(IEvent<T> ev)
    {
        Cml.Sync(ev, OnSync);
    }

    private void OnSync(T value)
    {
        // Written before the Exchange below, which is a full fence, so any thread
        // that observes the Sentinel also observes the result.
        _result = value;

        var c = Interlocked.Exchange(ref _continuation, Sentinel);

        // null  -> completed inline, before anyone asked to be resumed.
        // Sentinel -> impossible, we only ever complete once.
        // anything else -> the fiber parked; resume it.
        if (c != null && !ReferenceEquals(c, Sentinel)) c();
    }

    public bool IsCompleted => ReferenceEquals(Volatile.Read(ref _continuation), Sentinel);

    public T GetResult() => _result;

    public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

    /// <summary>
    /// "Unsafe" here means "does not flow ExecutionContext", which is precisely what
    /// BjoML wants: the fiber carries its own dynamic environment and the builder
    /// reinstates it on resume.
    /// </summary>
    public void UnsafeOnCompleted(Action continuation)
    {
        var prev = Interlocked.CompareExchange(ref _continuation, continuation, null);

        // The event completed in the gap between IsCompleted returning false and us
        // registering. Nobody will call us, so run it here.
        if (ReferenceEquals(prev, Sentinel)) continuation();
    }
}

/// <summary>Convenience constructors for the common channel operations.</summary>
public static class ChannelEvents
{
    public static ChannelReceiveOperation<T> Receive<T>(this Channel<T> ch) => ch.Receive();

    public static ChannelSendOperation<T> Send<T>(this Channel<T> ch, T value) => ch.Send(value);
}
