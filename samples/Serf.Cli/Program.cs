using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using MessagePack;

namespace Serf.Cli
{

    [MessagePackObject]
    public class CommandHeader
    {
        [Key(0)]
        public string Command { get; set; }

        [Key(1)]
        public uint Sequence { get; set; }
    }

    [MessagePackObject]
    public class Handshake
    {
        [Key(0)]
        public string Version { get; set; }
    }

    class Program
    {
        public static Task<int> Main(string[] args) => CommandLineApplication.ExecuteAsync<Program>(args);

        [Option(Description = "the rpc endpoints address", LongName = "rpc-addr")]
        public string Subject { get; } = "127.0.0.1:7373";

        private async Task OnExecuteAsync()
        {
            var uri = new Uri(Subject);

            using (var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            using (var myStream = new NetworkStream(s))
            {
                var split = Subject.Split(';');
                var ip = new IPEndPoint(IPAddress.Parse(split[0]), int.Parse(split[1]));

                s.Connect(ip);

                var header = new CommandHeader {Command = "handshake", Sequence = 0 };
                var bytes = MessagePackSerializer.Serialize(header);
                await myStream.WriteAsync(bytes);

                var newcommand = new Handshake { Version = "1" };
                bytes = MessagePackSerializer.Serialize(newcommand);
                await myStream.WriteAsync(bytes);

                var responseHeader = await MessagePackSerializer.DeserializeAsync<CommandHeader>(myStream);
            }
        }
    }
}
