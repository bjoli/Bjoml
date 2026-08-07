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

namespace Bjoml;

/// <summary>
/// The hosted language's ambient dynamic environment, as one opaque per-thread slot.
///
/// BjoML deliberately knows NOTHING about the payload. The language owns it, and the
/// runtime only ever saves the reference and puts it back, so installing a context is
/// a single pointer store. Whatever the language wants reachable from a fiber —
/// current ports, the parameterize map, a condition-handler stack — lives behind this
/// one reference.
///
/// WHY THIS AND NOT ExecutionContext / AsyncLocal
///
/// <c>AsyncLocal</c> rides on <see cref="System.Threading.ExecutionContext"/>, which
/// allocates a fresh context object on every write and is copied at every await.
/// For a language where <c>(parameterize ...)</c> is idiomatic and expected to be
/// cheap, that is the wrong cost model. BjoML therefore never flows EC at all
/// (every enqueue is an <c>Unsafe</c> one, every await is an unsafe await) and
/// propagates this slot by hand in the fiber builder instead.
///
/// WHY SAVE/RESTORE RATHER THAN ASSIGN
///
/// Continuations run inline on whichever thread completed a rendezvous. That thread
/// may be several frames deep inside a DIFFERENT fiber. So a resuming fiber borrows
/// the thread, and must hand it back exactly as it found it — see
/// <c>FiberStateMachineBox.Execute</c>.
///
/// CONSEQUENCE
///
/// <c>AsyncLocal</c> values set by C# libraries (Activity/OpenTelemetry spans, some
/// logging scopes) will NOT flow across a BjoML await. If you want tracing, carry
/// the span inside your own context object.
///
/// LIMITATION
///
/// The context is only captured and reinstated at *fiber* suspension points. A bare
/// callback handed to the scheduler from outside a fiber — a nack action, a promise
/// completion waiter — runs with whatever context the borrowed thread happened to
/// have. So such callbacks must never run user code; they should only wake a fiber
/// and let the fiber run it.
/// </summary>
public static class FiberContext
{
    [ThreadStatic]
    private static object? _current;

    /// <summary>
    /// The current thread's dynamic environment. Null means "no fiber context",
    /// which is what a raw thread-pool thread starts with.
    /// </summary>
    public static object? Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => _current = value;
    }

    /// <summary>
    /// Scoped install, for compiling <c>(parameterize ...)</c>:
    ///
    /// <code>
    /// using (FiberContext.Push(saved.With(output: newPort)))
    /// {
    ///     ... body, may contain awaits ...
    /// }
    /// </code>
    ///
    /// This stays correct across an await ONLY because the fiber builder re-installs
    /// the captured context when the fiber resumes on another thread.
    /// </summary>
    public readonly ref struct Scope
    {
        private readonly object? _saved;

        public Scope(object? next)
        {
            _saved = _current;
            _current = next;
        }

        public void Dispose() => _current = _saved;
    }

    public static Scope Push(object? next) => new Scope(next);
}
