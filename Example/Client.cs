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
using System.Threading.Tasks;
using Bjoml;

public static class MyClient
{
    public static Task Client(Channel<object> @in, Channel<object> @out)
    {
        ClientStateMachine stateMachine = new ClientStateMachine();
        stateMachine._builder = AsyncTaskMethodBuilder.Create();
        var task = stateMachine._builder.Task;
        stateMachine.@in = @in;
        stateMachine.@out = @out;
        stateMachine._state = -1;
        stateMachine._builder.Start(ref stateMachine);
        return task;
    }

    [CompilerGenerated]
    private struct ClientStateMachine : IAsyncStateMachine
    {
        public int _state;
        public AsyncTaskMethodBuilder _builder;
        public Channel<object> @in;
        public Channel<object> @out;

        private object[] _messages;
        private int _index;

        // The awaiters a direct `await ch.Send(v)` / `await ch.Receive()` resolves
        // to: the channel's own, which park an operation without going through
        // `Cml.Sync` at all. This is the path the compiled language takes.
        private ChannelSendAwaiter<object> _u1;
        private ChannelReceiveAwaiter<object> _u2;

        public void MoveNext()
        {
            int num = _state;
            try
            {
                ChannelSendAwaiter<object> putAwaiter;
                ChannelReceiveAwaiter<object> getAwaiter;

                if (num == 0)
                {
                    putAwaiter = _u1;
                    _u1 = default;
                    num = (_state = -1);
                    goto Label_Send_Completed;
                }
                if (num == 1)
                {
                    getAwaiter = _u2;
                    _u2 = default;
                    num = (_state = -1);
                    goto Label_Receive_Completed;
                }

                // Initial setup
                _messages = new object[] { "ping!", "sup" };
                _index = 0;

            Label_Loop_Start:
                if (_index >= _messages.Length) goto Label_Loop_End;

                var msg = _messages[_index];
                putAwaiter = @out.Send(msg).GetAwaiter();
                if (!putAwaiter.IsCompleted)
                {
                    num = (_state = 0);
                    _u1 = putAwaiter;
                    ClientStateMachine stateMachine = this;
                    _builder.AwaitUnsafeOnCompleted(ref putAwaiter, ref stateMachine);
                    return; // Yield thread
                }

            Label_Send_Completed:
                putAwaiter.GetResult();

                getAwaiter = @in.Receive().GetAwaiter();
                if (!getAwaiter.IsCompleted)
                {
                    num = (_state = 1);
                    _u2 = getAwaiter;
                    ClientStateMachine stateMachine = this;
                    _builder.AwaitUnsafeOnCompleted(ref getAwaiter, ref stateMachine);
                    return; // Yield thread
                }

            Label_Receive_Completed:
                var response = getAwaiter.GetResult();
                Console.WriteLine($"client-received: {response}");

                _index++;
                goto Label_Loop_Start;
            
            Label_Loop_End:;
            }
            catch (Exception exception)
            {
                _state = -2;
                _builder.SetException(exception);
                return;
            }

            _state = -2;
            Console.WriteLine("Client calling SetResult");
            _builder.SetResult();
            Console.WriteLine("Client SetResult returned");
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine)
        {
            _builder.SetStateMachine(stateMachine);
        }
    }
}
