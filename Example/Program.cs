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