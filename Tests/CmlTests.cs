// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
// Part of BjoML. LGPL-3.0-or-later.
//
// Regression tests for the correctness bugs described in the review.

using System;
using System.Threading;
using static Bjoml.Tests.Harness;

namespace Bjoml.Tests;

public static class CmlTests
{
    public static void RunAll()
    {
        Section("B3 - self-synchronisation must not livelock");
        Run("choose(send ch, recv ch) publishes without spinning", SelfChooseTerminates);
        Run("self-choose still pairs with an external partner", SelfChoosePairsExternally);
        Run("two symmetric self-chooses pair with each other", SymmetricSelfChooses);

        Section("B1 - nack must not fire when a nested choose under it wins");
        Run("nack silent when its own subtree wins", NackSilentWhenSubtreeWins);
        Run("nack fires when an outside branch wins", NackFiresWhenOutsideBranchWins);

        Section("B2 - colliding nack ids must not overwrite each other");
        Run("two nested withNacks both fire", NestedNacksBothFire);

        Section("B4 / B10 - always-enabled events");
        Run("Always wins a choose", AlwaysWinsChoose);
        Run("Always is not dropped when published after a parked branch", AlwaysAfterParkedBranch);

        Section("B7 - losing branches must stay bounded, live ones must survive");
        Run("losers do not accumulate in an idle channel", StaleOpsDoNotAccumulate);
        Run("a channel never touched again is still cleaned", IdleChannelCleanedWithoutTraffic);
        Run("a live parked receive is NOT cleaned away", LiveReceiveSurvivesCleanup);
        Run("withNack losers stay bounded too", WithNackLosersBounded);

        Section("Baseline combinator behaviour");
        Run("wrap maps the value", WrapMapsValue);
        Run("guard is evaluated at sync time", GuardIsDeferred);
    }

    // -----------------------------------------------------------------------
    // B3
    // -----------------------------------------------------------------------

    /// <summary>
    /// Before the fix this never returned: the receive branch dequeued the send
    /// branch's own op, claimed the shared state W-&gt;C, then failed to drive that
    /// same state W-&gt;S, reset, re-queued and retried forever. Reaching the end of
    /// this method at all is the assertion.
    /// </summary>
    private static void SelfChooseTerminates()
    {
        var ch = new Channel<int>();
        var published = new ManualResetEventSlim(false);

        Cml.Sync(
            Cml.Choose(
                Cml.Wrap(new ChannelSendEvent<int>(ch, 42), _ => "sent"),
                Cml.Wrap(new ChannelReceiveEvent<int>(ch), v => $"got {v}")),
            _ => { });

        published.Set();
        Await(published, "publish to return", 1000);
    }

    /// <summary>
    /// The self-choose must not merely avoid spinning, it must stay live: both of
    /// its branches remain genuinely offered to other threads.
    /// </summary>
    private static void SelfChoosePairsExternally()
    {
        var ch = new Channel<int>();
        var chooserDone = new ManualResetEventSlim(false);
        var partnerDone = new ManualResetEventSlim(false);
        string? chooserResult = null;
        int received = -1;

        Cml.Sync(
            Cml.Choose(
                Cml.Wrap(new ChannelSendEvent<int>(ch, 42), _ => "sent"),
                Cml.Wrap(new ChannelReceiveEvent<int>(ch), v => $"got {v}")),
            r => { chooserResult = r; chooserDone.Set(); });

        // An outside receiver should take the send branch.
        Cml.Sync(new ChannelReceiveEvent<int>(ch), v => { received = v; partnerDone.Set(); });

        Await(chooserDone, "the self-choose to commit");
        Await(partnerDone, "the external receiver to commit");

        AssertEqual("sent", chooserResult, "self-choose picked the wrong branch");
        AssertEqual(42, received, "partner received the wrong value");
    }

    /// <summary>Two sync blocks that each offer both directions must pair with each other.</summary>
    private static void SymmetricSelfChooses()
    {
        var ch = new Channel<int>();
        var first = new ManualResetEventSlim(false);
        var second = new ManualResetEventSlim(false);

        Cml.Sync(
            Cml.Choose(
                Cml.Wrap(new ChannelSendEvent<int>(ch, 1), _ => "sent"),
                Cml.Wrap(new ChannelReceiveEvent<int>(ch), v => $"got {v}")),
            _ => first.Set());

        Cml.Sync(
            Cml.Choose(
                Cml.Wrap(new ChannelSendEvent<int>(ch, 2), _ => "sent"),
                Cml.Wrap(new ChannelReceiveEvent<int>(ch), v => $"got {v}")),
            _ => second.Set());

        Await(first, "the first symmetric choose");
        Await(second, "the second symmetric choose");
    }

    // -----------------------------------------------------------------------
    // B1
    // -----------------------------------------------------------------------

    /// <summary>
    /// choose(withNack(choose(recvA, recvB)), recvC), and A wins.
    ///
    /// The winning id is a grandchild of the withNack's id. With a flat id set the
    /// winner simply "is not my id", so the nack fired even though the withNack's
    /// own subtree committed — in a server protocol, cancelling the very request
    /// you just succeeded at.
    /// </summary>
    private static void NackSilentWhenSubtreeWins()
    {
        var a = new Channel<int>();
        var b = new Channel<int>();
        var c = new Channel<int>();

        var nackFired = new ManualResetEventSlim(false);
        var done = new ManualResetEventSlim(false);
        string? result = null;

        Cml.Sync(
            Cml.Choose(
                Cml.WithNack(nack =>
                {
                    Cml.Sync(nack, _ => nackFired.Set());
                    return Cml.Choose(
                        Cml.Wrap(new ChannelReceiveEvent<int>(a), v => $"a{v}"),
                        Cml.Wrap(new ChannelReceiveEvent<int>(b), v => $"b{v}"));
                }),
                Cml.Wrap(new ChannelReceiveEvent<int>(c), v => $"c{v}")),
            r => { result = r; done.Set(); });

        Cml.Sync(new ChannelSendEvent<int>(a, 1), _ => { });

        Await(done, "the choose to commit");
        AssertEqual("a1", result, "wrong branch won");
        AssertNoSignal(nackFired, "the nack of the winning withNack subtree fired");
    }

    private static void NackFiresWhenOutsideBranchWins()
    {
        var a = new Channel<int>();
        var b = new Channel<int>();
        var c = new Channel<int>();

        var nackFired = new ManualResetEventSlim(false);
        var done = new ManualResetEventSlim(false);
        string? result = null;

        Cml.Sync(
            Cml.Choose(
                Cml.WithNack(nack =>
                {
                    Cml.Sync(nack, _ => nackFired.Set());
                    return Cml.Choose(
                        Cml.Wrap(new ChannelReceiveEvent<int>(a), v => $"a{v}"),
                        Cml.Wrap(new ChannelReceiveEvent<int>(b), v => $"b{v}"));
                }),
                Cml.Wrap(new ChannelReceiveEvent<int>(c), v => $"c{v}")),
            r => { result = r; done.Set(); });

        Cml.Sync(new ChannelSendEvent<int>(c, 9), _ => { });

        Await(done, "the choose to commit");
        AssertEqual("c9", result, "wrong branch won");
        Await(nackFired, "the losing withNack subtree's nack");
    }

    // -----------------------------------------------------------------------
    // B2
    // -----------------------------------------------------------------------

    /// <summary>
    /// withNack publishes its generated event with its OWN id, so a withNack nested
    /// (through a wrap) inside another registers a second nack under the same id.
    /// Keying nacks by id meant the inner one overwrote the outer one and the outer
    /// nack was silently never fired.
    /// </summary>
    private static void NestedNacksBothFire()
    {
        var a = new Channel<int>();
        var b = new Channel<int>();

        var outerFired = new ManualResetEventSlim(false);
        var innerFired = new ManualResetEventSlim(false);
        var done = new ManualResetEventSlim(false);

        Cml.Sync(
            Cml.Choose(
                Cml.WithNack(outer =>
                {
                    Cml.Sync(outer, _ => outerFired.Set());
                    return Cml.Wrap(
                        Cml.WithNack(inner =>
                        {
                            Cml.Sync(inner, _ => innerFired.Set());
                            return new ChannelReceiveEvent<int>(a);
                        }),
                        v => $"a{v}");
                }),
                Cml.Wrap(new ChannelReceiveEvent<int>(b), v => $"b{v}")),
            _ => done.Set());

        Cml.Sync(new ChannelSendEvent<int>(b, 7), _ => { });

        Await(done, "the choose to commit");
        Await(innerFired, "the inner nack");
        Await(outerFired, "the outer nack (overwritten by the inner one before the fix)");
    }

    // -----------------------------------------------------------------------
    // B4 / B10
    // -----------------------------------------------------------------------

    private static void AlwaysWinsChoose()
    {
        var idle = new Channel<int>();
        var done = new ManualResetEventSlim(false);
        string? result = null;

        Cml.Sync(
            Cml.Choose(
                Cml.Wrap(new ChannelReceiveEvent<int>(idle), v => $"chan{v}"),
                Cml.Always("always")),
            r => { result = r; done.Set(); });

        Await(done, "Always to commit");
        AssertEqual("always", result, "Always did not win against an idle channel");
    }

    /// <summary>
    /// Always is published second, after a channel branch has already parked an
    /// operation. It must still commit rather than find the state busy and vanish.
    /// </summary>
    private static void AlwaysAfterParkedBranch()
    {
        var idle = new Channel<int>();

        for (int i = 0; i < 200; i++)
        {
            var done = new ManualResetEventSlim(false);
            Cml.Sync(
                Cml.Choose(
                    Cml.Wrap(new ChannelReceiveEvent<int>(idle), v => $"chan{v}"),
                    Cml.Always("always")),
                _ => done.Set());

            Await(done, $"Always to commit on iteration {i}", 2000);
        }
    }

    // -----------------------------------------------------------------------
    // B7
    // -----------------------------------------------------------------------

    /// <summary>
    /// The core B7 assertion: a channel offered in a choose that keeps LOSING must not
    /// accumulate the dead operations of those losing branches.
    ///
    /// Note this reads <c>RawPendingReceiveCount</c>, not <c>PendingReceiveCount</c>.
    /// The latter cleans before counting, so it reports 0 whether or not the bug is
    /// fixed and cannot witness the leak at all.
    /// </summary>
    private static void StaleOpsDoNotAccumulate()
    {
        const int iterations = 500;

        var busy = new Channel<int>();
        var idle = new Channel<int>();

        for (int i = 0; i < iterations; i++)
        {
            var done = new ManualResetEventSlim(false);

            Cml.Sync(
                Cml.Choose(
                    new ChannelReceiveEvent<int>(busy),
                    new ChannelReceiveEvent<int>(idle)),
                _ => done.Set());

            Cml.Sync(new ChannelSendEvent<int>(busy, i), _ => { });
            Await(done, $"iteration {i}", 2000);
        }

        int stranded = idle.RawPendingReceiveCount;

        // Not asserting exactly 0: a commit races with the losing branch still being
        // published, so the last iteration's loser may legitimately still be in flight.
        // The property that matters is that it does not GROW with the iteration count.
        Assert(stranded <= 2,
            $"Expected losing branches to be reclaimed, but {stranded} of {iterations} are stranded");
    }

    /// <summary>
    /// The case that makes B7 nasty, and the reason cleanup must be driven from the
    /// committing side: a channel that is offered once and then never sees traffic
    /// again. Anything triggered by channel activity cannot reach it, because there is
    /// no subsequent activity.
    /// </summary>
    private static void IdleChannelCleanedWithoutTraffic()
    {
        var busy = new Channel<int>();
        var idle = new Channel<int>();
        var done = new ManualResetEventSlim(false);

        Cml.Sync(
            Cml.Choose(
                new ChannelReceiveEvent<int>(busy),
                new ChannelReceiveEvent<int>(idle)),
            _ => done.Set());

        Cml.Sync(new ChannelSendEvent<int>(busy, 1), _ => { });
        Await(done, "the busy branch to win", 2000);

        // `idle` is now never touched again. Give the commit-driven cleanup a moment,
        // then confirm the loser was reclaimed anyway.
        Thread.Sleep(100);

        int stranded = idle.RawPendingReceiveCount;
        Assert(stranded == 0,
            $"A channel with no further traffic kept {stranded} dead entries");
    }

    /// <summary>
    /// The safety side of the fix, and the one that would catch an over-eager cleanup:
    /// an operation whose block has NOT committed is still live and must survive.
    /// Reclaiming it would lose a genuine waiter and hang the receiver forever.
    /// </summary>
    private static void LiveReceiveSurvivesCleanup()
    {
        var live = new Channel<int>();
        var busy = new Channel<int>();
        var received = new ManualResetEventSlim(false);
        int got = -1;

        // A plain receive that nobody will satisfy yet. This must stay parked.
        Cml.Sync(new ChannelReceiveEvent<int>(live), v => { got = v; received.Set(); });

        // Churn unrelated chooses so cleanup runs repeatedly against `live`.
        for (int i = 0; i < 50; i++)
        {
            var done = new ManualResetEventSlim(false);
            Cml.Sync(
                Cml.Choose(
                    new ChannelReceiveEvent<int>(busy),
                    new ChannelReceiveEvent<int>(live)),
                _ => done.Set());

            Cml.Sync(new ChannelSendEvent<int>(busy, i), _ => { });
            Await(done, $"iteration {i}", 2000);
        }

        // The original live receive must still be there and must still work.
        Cml.Sync(new ChannelSendEvent<int>(live, 99), _ => { });
        Await(received, "the still-parked receive to be satisfied", 2000);
        AssertEqual(99, got, "value delivered to the surviving receive");
    }

    /// <summary>
    /// withNack builds a deeper event tree, so the losing branch is published through
    /// a different path. Confirm registration still happens there.
    /// </summary>
    private static void WithNackLosersBounded()
    {
        const int iterations = 200;

        var busy = new Channel<int>();
        var idle = new Channel<int>();

        for (int i = 0; i < iterations; i++)
        {
            var done = new ManualResetEventSlim(false);

            Cml.Sync(
                Cml.Choose(
                    new ChannelReceiveEvent<int>(busy),
                    Cml.WithNack(nack =>
                    {
                        Cml.Sync(nack, _ => { });
                        return new ChannelReceiveEvent<int>(idle);
                    })),
                _ => done.Set());

            Cml.Sync(new ChannelSendEvent<int>(busy, i), _ => { });
            Await(done, $"iteration {i}", 2000);
        }

        int stranded = idle.RawPendingReceiveCount;
        Assert(stranded <= 2,
            $"withNack losing branches stranded {stranded} of {iterations}");
    }

    // -----------------------------------------------------------------------
    // Sanity
    // -----------------------------------------------------------------------

    private static void WrapMapsValue()
    {
        var ch = new Channel<int>();
        var done = new ManualResetEventSlim(false);
        string? result = null;

        Cml.Sync(Cml.Wrap(new ChannelReceiveEvent<int>(ch), v => $"<{v}>"),
                 r => { result = r; done.Set(); });

        Cml.Sync(new ChannelSendEvent<int>(ch, 5), _ => { });

        Await(done, "the wrapped receive");
        AssertEqual("<5>", result, "wrap did not map the value");
    }

    private static void GuardIsDeferred()
    {
        int generated = 0;
        var guard = Cml.Guard(() => { generated++; return Cml.Always(1); });

        AssertEqual(0, generated, "guard ran before sync");

        var done = new ManualResetEventSlim(false);
        Cml.Sync(guard, _ => done.Set());

        Await(done, "the guarded event");
        AssertEqual(1, generated, "guard generator ran the wrong number of times");
    }
}
