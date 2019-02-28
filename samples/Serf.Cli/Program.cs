using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Serf.Client;

namespace Serf.Cli
{
    class BaseCommand : IDisposable
    {
        TcpClient tcpClient;
        SerfClient srfClient;

        [Option(Description = "the rpc endpoints address", LongName = "rpc-addr")]
        public string Subject { get; } = "127.0.0.1:7373";

        [Option(Description = "rpc authentication key", LongName = "rpc-auth")]
        public string Authenticationkey { get; } = null;

        public void Dispose()
        {
            srfClient?.Dispose(true);
            tcpClient?.Dispose(true);
        }

        protected async Task<SerfClient> GetClient()
        {
            var splitHost = Subject.Split(':');
            var hostPortion = splitHost[0];
            var port = int.Parse(splitHost[1]);

            tcpClient = new TcpClient(hostPortion, port);
            srfClient = new SerfClient(tcpClient.GetStream());

            var handshakeError = await srfClient.Handshake();
            if (handshakeError != null)
            {
                throw new Exception($"failed during handshake: {handshakeError.Error}");
            }

            if (!string.IsNullOrWhiteSpace(Authenticationkey))
            {
                var authenticationError = await srfClient.Authenticate(Authenticationkey);
                if (authenticationError != null)
                {
                    throw new Exception($"failed during authentication: {authenticationError.Error}");
                }
                
            }

            return srfClient;
        }
    }

    [Command("join")]
    class JoinCommand : BaseCommand
    {
        [Required(ErrorMessage = "You must specify at least one peer")]
        [Argument(0, Description = "The peer to use for connection [\"10.0.0.2:7373\"]")]
        public IEnumerable<string> Peers { get; }

        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var (joinedNodes, error) = await client.Join(Peers);
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to join the cluster though {Peers.Aggregate((x,y) => $"{x}, {y}")}: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync($"Joined the cluster through {joinedNodes} peers.");

                return 0;
            }
        }
    }

    [Command("leave")]
    class LeaveCommand : BaseCommand
    {
        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var error = await client.Leave();
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to leave the cluster: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync($"Left the cluster.");
                return 0;
            }
        }
    }

    [Command("keys")]
    [Subcommand(typeof(InstallKeyCommand), typeof(UseKeyCommand), typeof(RemoveKeyCommand))]
    class KeysCommand : BaseCommand
    {
        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var (listResponse, error) = await client.ListKeys();
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to list keys: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync("Keys installed in the cluster:");
                foreach (var key in listResponse.Keys)
                {
                    await console.Out.WriteLineAsync($"{key.Key} : {key.Value}");
                }

                return 0;
            }
        }
    }

    [Command("install")]
    class InstallKeyCommand : BaseCommand
    {
        [Required(ErrorMessage = "You must specify the key")]
        [Argument(0, Description = "The key (16 bytes of base64-encoded data)")]
        public string Key { get; }

        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var (response, error) = await client.InstallKey(Key);
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to install key: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync($"Installed Keys in the cluster");
                return 0;
            }
        }
    }

    [Command("use")]
    class UseKeyCommand : BaseCommand
    {
        [Required(ErrorMessage = "You must specify the key")]
        [Argument(0, Description = "The key (16 bytes of base64-encoded data)")]
        public string Key { get; }

        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var (response, error) = await client.UseKey(Key);
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to switch to the given key: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync($"The cluster das successfully switched to key {Key}");
                return 0;
            }
        }
    }

    [Command("remove")]
    class RemoveKeyCommand : BaseCommand
    {
        [Required(ErrorMessage = "You must specify the key")]
        [Argument(0, Description = "The key (16 bytes of base64-encoded data)")]
        public string Key { get; }

        protected async Task<int> OnExecute(IConsole console)
        {
            using (var client = await GetClient())
            {
                var (response, error) = await client.RemoveKey(Key);
                if (error != null)
                {
                    await console.Error.WriteLineAsync($"Failed to remove the given key: {error.Error}");
                    return 1;
                }

                await console.Out.WriteLineAsync($"{Key} was removed");
                return 0;
            }
        }
    }

    [Command("serf-cli")]
    [Subcommand(typeof(JoinCommand), typeof(LeaveCommand), typeof(KeysCommand))]
    class Program
    {
        public static Task<int> Main(string[] args) => CommandLineApplication.ExecuteAsync<Program>(args);


        void OnExecuteAsync(CommandLineApplication app)
        {
            app.ShowHelp();
        }
    }
}
