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

        Section("B7 - stale operations in channel queues (KNOWN ISSUE, not yet fixed)");
        Run("characterise: losing branches accumulate in an idle channel", StaleOpsAccumulate);

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
    /// CHARACTERISATION TEST — asserts the CURRENT, BUGGY behaviour on purpose.
    ///
    /// When a choose branch loses, its PutOp/GetOp stays in the channel queue. It is
    /// only reclaimed if some LATER operation happens to dequeue it and notice it is
    /// synchronized. So a channel that is offered in a choose but never actually
    /// communicated on grows without bound, and the operation pool never gets its
    /// objects back.
    ///
    /// Fixing this needs O(1) removable queue entries (an intrusive linked list) plus
    /// SyncState tracking its published ops so it can unlink the losers. That is a
    /// rewrite of the channel core, so it is deliberately NOT done here.
    ///
    /// When someone does fix it, this test will fail — which is the point. Change it
    /// to assert the queue stays bounded.
    /// </summary>
    private static void StaleOpsAccumulate()
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

        int stranded = idle.PendingReceiveCount;

        Assert(stranded >= iterations,
            $"B7 appears to be FIXED: only {stranded} stale ops after {iterations} losing " +
            "branches. Update this characterisation test to assert boundedness instead.");
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
