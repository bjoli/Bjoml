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

        private TaskAwaiter _u1;
        private TaskAwaiter<object> _u2;

        public void MoveNext()
        {
            int num = _state;
            try
            {
                TaskAwaiter putAwaiter;
                TaskAwaiter<object> getAwaiter;

                if (num == 0)
                {
                    putAwaiter = _u1;
                    _u1 = default;
                    num = (_state = -1);
                    goto Label_PutMessage_Completed;
                }
                if (num == 1)
                {
                    getAwaiter = _u2;
                    _u2 = default;
                    num = (_state = -1);
                    goto Label_GetMessage_Completed;
                }

                // Initial setup
                _messages = new object[] { "ping!", "sup" };
                _index = 0;

            Label_Loop_Start:
                if (_index >= _messages.Length) goto Label_Loop_End;

                var msg = _messages[_index];
                putAwaiter = @out.PutMessage(msg).GetAwaiter();
                if (!putAwaiter.IsCompleted)
                {
                    num = (_state = 0);
                    _u1 = putAwaiter;
                    ClientStateMachine stateMachine = this;
                    _builder.AwaitUnsafeOnCompleted(ref putAwaiter, ref stateMachine);
                    return; // Yield thread
                }

            Label_PutMessage_Completed:
                putAwaiter.GetResult();

                getAwaiter = @in.GetMessage().GetAwaiter();
                if (!getAwaiter.IsCompleted)
                {
                    num = (_state = 1);
                    _u2 = getAwaiter;
                    ClientStateMachine stateMachine = this;
                    _builder.AwaitUnsafeOnCompleted(ref getAwaiter, ref stateMachine);
                    return; // Yield thread
                }

            Label_GetMessage_Completed:
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
