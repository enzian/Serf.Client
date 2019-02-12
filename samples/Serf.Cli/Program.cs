using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using MessagePack;

namespace Serf.Cli
{

    [MessagePackObject]
    public class CommandHeader
    {
        [Key("Command")]
        public string Command { get; set; }

        [Key("Seq")]
        public ulong Sequence { get; set; }
    }

    [MessagePackObject]
    public struct ResponseHeader
    {
        [Key("Seq")]
        public ulong Seq;

        [Key("Error")]
        public string Error;
    }

    [MessagePackObject]
    public class Handshake
    {
        [Key("Version")]
        public int Version { get; set; }
    }

    [MessagePackObject]
    public class Join
    {
        [Key("Existing")]
        public string[] Existing { get; set; }

        [Key("Replay")]
        public bool Replay { get; set; }
    }

    class Program
    {
        public static Task<int> Main(string[] args) => CommandLineApplication.ExecuteAsync<Program>(args);

        [Option(Description = "the rpc endpoints address", LongName = "rpc-addr")]
        public string Subject { get; } = "127.0.0.1:7373";

        private async Task OnExecuteAsync()
        {
            using (var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                var split = Subject.Split(':');
                var ip = new IPEndPoint(IPAddress.Parse(split[0]), int.Parse(split[1]));

                s.Connect(ip);

                using (var myStream = new NetworkStream(s))
                {
                    var task = ReadResponses(myStream);

                    var header = new CommandHeader { Command = "handshake", Sequence = 0 };
                    var bytes = MessagePackSerializer.Serialize(header);
                    await myStream.WriteAsync(bytes);

                    await myStream.FlushAsync();

                    var newcommand = new Handshake { Version = 1 };
                    bytes = MessagePackSerializer.Serialize(newcommand);
                    await myStream.WriteAsync(bytes);

                    await myStream.FlushAsync();


                    var membersHeader = new CommandHeader { Command = "join", Sequence = 1 };
                    var memberHeadbytes = MessagePackSerializer.Serialize(membersHeader);
                    await myStream.WriteAsync(memberHeadbytes);

                    await myStream.FlushAsync();

                    var joinCommand = new Join { Existing = new[] { "test:8080" }, Replay = false };
                    var joinbytes = MessagePackSerializer.Serialize(joinCommand);
                    await myStream.WriteAsync(joinbytes);

                    await myStream.FlushAsync();

                    //var responseHeader = await MessagePackSerializer.DeserializeAsync<ResponseHeader>(myStream);

                    task.Wait();
                }
            }
        }

        private async Task ReadResponses(NetworkStream ns)
        {
            while(true)
            {
                try
                {
                    var buffer = new byte[8048];

                    var size = await ns.ReadAsync(buffer);
                    var str = Encoding.UTF8.GetString(buffer.Take(size).ToArray());

                    var responseHeader = MessagePackSerializer.Deserialize<ResponseHeader>(buffer.Take(size).ToArray());
                    Console.WriteLine($"Response received for sequence {responseHeader.Seq}: {responseHeader.Error}");
                }
                catch (Exception)
                {
                    break;
                }
                
            }
        }
    }
}
