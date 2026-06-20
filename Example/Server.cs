using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Bjoml;

public static class MyServer
{
    // The original method is rewritten to initialize and start the state machine
    public static Task Server(Channel<object> @in, Channel<object> @out)
    {
        ServerStateMachine stateMachine = new ServerStateMachine();
        stateMachine._builder = AsyncTaskMethodBuilder.Create();
        var task = stateMachine._builder.Task;
        stateMachine.@in = @in;
        stateMachine.@out = @out;
        stateMachine._state = -1;
        stateMachine._builder.Start(ref stateMachine);
        return task;
    }

    // The compiler-generated state machine struct
    [CompilerGenerated]
    private struct ServerStateMachine : IAsyncStateMachine
    {
        public int _state;
        public AsyncTaskMethodBuilder _builder;
        public Channel<object> @in;
        public Channel<object> @out;
        
        // Local variables hoisted to struct fields
        private object _msg; 
        
        // Awaiter fields
        private TaskAwaiter<object> _u1;
        private TaskAwaiter _u2;

        public void MoveNext()
        {
            int num = _state;
            try
            {
                TaskAwaiter<object> getAwaiter;
                TaskAwaiter putAwaiter;
                
                while (true) // The original while(true) loop
                {
                    switch (num)
                    {
                        case 0: // Resuming from GetMessage
                            getAwaiter = _u1;
                            _u1 = default;
                            num = (_state = -1);
                            goto Label_GetMessage_Completed;

                        case 1: // Resuming from PutMessage (ping)
                        case 2: // Resuming from PutMessage (sup)
                        case 3: // Resuming from PutMessage (wat)
                            putAwaiter = _u2;
                            _u2 = default;
                            num = (_state = -1);
                            goto Label_PutMessage_Completed;
                    }

                    // --- INITIAL EXECUTION / LOOP RESTART ---
                    getAwaiter = @in.GetMessage().GetAwaiter();
                    if (!getAwaiter.IsCompleted)
                    {
                        num = (_state = 0);
                        _u1 = getAwaiter;
                        ServerStateMachine stateMachine = this;
                        _builder.AwaitUnsafeOnCompleted(ref getAwaiter, ref stateMachine);
                        return; // Yield thread
                    }

                Label_GetMessage_Completed:
                    _msg = getAwaiter.GetResult();
                    Console.WriteLine($"server-received: {_msg}");

                    // --- BRANCHING LOGIC ---
                    if (_msg is "ping!")
                    {
                        putAwaiter = @out.PutMessage("pong!").GetAwaiter();
                        if (!putAwaiter.IsCompleted)
                        {
                            num = (_state = 1);
                            _u2 = putAwaiter;
                            ServerStateMachine stateMachine = this;
                            _builder.AwaitUnsafeOnCompleted(ref putAwaiter, ref stateMachine);
                            return; // Yield thread
                        }
                    }
                    else if (_msg is "sup")
                    {
                        putAwaiter = @out.PutMessage("not-much-u").GetAwaiter();
                        if (!putAwaiter.IsCompleted)
                        {
                            num = (_state = 2);
                            _u2 = putAwaiter;
                            ServerStateMachine stateMachine = this;
                            _builder.AwaitUnsafeOnCompleted(ref putAwaiter, ref stateMachine);
                            return; // Yield thread
                        }
                    }
                    else
                    {
                        putAwaiter = @out.PutMessage($"wat {_msg}").GetAwaiter();
                        if (!putAwaiter.IsCompleted)
                        {
                            num = (_state = 3);
                            _u2 = putAwaiter;
                            ServerStateMachine stateMachine = this;
                            _builder.AwaitUnsafeOnCompleted(ref putAwaiter, ref stateMachine);
                            return; // Yield thread
                        }
                    }

                Label_PutMessage_Completed:
                    putAwaiter.GetResult(); // Throw if faulted
                    
                    // The switch ends, the while(true) loop restarts, pulling the next message.
                }
            }
            catch (Exception exception)
            {
                _state = -2;
                _builder.SetException(exception);
            }
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine)
        {
            _builder.SetStateMachine(stateMachine);
        }
    }
}