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

namespace Bjoml;

public static class ChannelExtensions
{
    public static ValueTask<T> GetMessage<T>(this Channel<T> channel)
    {
        return Cml.SyncAsync(new ChannelReceiveEvent<T>(channel));
    }

    public static ValueTask PutMessage<T>(this Channel<T> channel, T value)
    {
        return Cml.SyncAsyncVoid(new ChannelSendEvent<T>(channel, value));
    }
}
