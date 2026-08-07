// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
// Part of BjoML. LGPL-3.0-or-later.

using System;

namespace Bjoml.Tests;

public static class Program
{
    public static int Main()
    {
        Scheduler.Start();

        Console.WriteLine("BjoML regression tests");

        CmlTests.RunAll();
        FiberTests.RunAll();
        SpawnBatchTests.RunAll();

        return Harness.Report();
    }
}
