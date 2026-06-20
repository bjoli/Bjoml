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

using System.Threading.Tasks;
using Bjoml;

namespace Example;

class Program
{
    static async Task Main(string[] args)
    {
        Scheduler.Start();
        
        var c2s = new Channel<object>();
        var s2c = new Channel<object>();

        var serverTask = MyServer.Server(c2s, s2c);
        var clientTask = MyClient.Client(s2c, c2s);

        await clientTask;
        Console.WriteLine("Done");
        Environment.Exit(0);
    }
}