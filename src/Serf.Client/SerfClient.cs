using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Serf.Client
{
    class TransactionContext
    {
        public CancellationTokenSource CancellationTokenSource { get; set; }

        public ResponseHeader Header { get; set; }

        public byte[] ResponseBuffer { get; set; }
    }

    public class SerfClient : ISerfClient, IDisposable
    {
        readonly NetworkStream _transportStream;

        readonly CancellationTokenSource _cancellationTokenSource;
        readonly Task _responseReaderTask;
        IDictionary<ulong, TransactionContext> handlers = new ConcurrentDictionary<ulong, TransactionContext>();
        long internalSeqence;
        readonly Socket _socket;

        public SerfClient(NetworkStream transportStream)
        {
            _transportStream = transportStream;

            _cancellationTokenSource = new CancellationTokenSource();
            _responseReaderTask = ReadResponses(_cancellationTokenSource.Token);
        }
        public SerfClient(Socket socket) : this(new NetworkStream(socket))
        {
            _socket = socket;
        }

        public Task<SerfError> Handshake()
        {
            return HandleRequest("handshake", new Handshake { Version = 1 });
        }

        public Task<SerfError> Authenticate(string secret)
        {
            return HandleRequest("auth", new Authentication { AuthenticationKey = secret });
        }

        public async Task<(uint joinedNodes, SerfError error)> Join(IEnumerable<string> members, bool replay = false)
        {
            var join = new JoinRequest { Existing = members.ToArray(), Replay = replay };

            var (joinResponse, error) = await HandleRequestReponse<JoinRequest, JoinResponse>("join", join);

            return (joinResponse.Peers, error);
        }

        public async Task<(IEnumerable<Members>, SerfError error)> Members()
        {
            var (response, error) = await HandleRequestReponse<MembersRequest, MembersResponse>("members", null);

            return (response?.Members, error);
        }

        public Task<SerfError> Leave()
        {
            return HandleRequest<LeaveRequest>("leave", null);
        }


        public Task<(KeyActionResponse response, SerfError error)> InstallKey(string key)
        {
            return HandleRequestReponse<KeyRequest, KeyActionResponse>("install-key", new KeyRequest { Key = key });
        }

        public Task<(KeyActionResponse response, SerfError error)> UseKey(string key)
        {
            return HandleRequestReponse<KeyRequest, KeyActionResponse>("use-key", new KeyRequest { Key = key });
        }

        public Task<(KeyActionResponse response, SerfError error)> RemoveKey(string key)
        {
            return HandleRequestReponse<KeyRequest, KeyActionResponse>("remove-key", new KeyRequest { Key = key });
        }

        public Task<(KeyListResponse response, SerfError error)> ListKeys()
        {
            return HandleRequestReponse<KeyRequest, KeyListResponse>("list-keys", null);
        }

        async Task<(TResponse, SerfError)> HandleRequestReponse<TRequest, TResponse>(string command, TRequest request)
            where TRequest : class
        {
            var sequence = (ulong)Interlocked.Increment(ref internalSeqence);
            var reset = new ManualResetEventSlim(false);

            var cancellationToken = new CancellationTokenSource();
            SerfError error = null;
            TResponse response = default(TResponse);

            var transaction = new TransactionContext { CancellationTokenSource = cancellationToken };
            handlers.Add(sequence, transaction);

            cancellationToken.Token.Register(() =>
            {
                if (!string.IsNullOrWhiteSpace(transaction.Header.Error))
                {
                    error = new SerfError { Error = transaction.Header.Error };
                }
                else
                {
                    var resolver = MessagePack.Resolvers.StandardResolver.Instance;
                    var formatter = resolver.GetFormatterWithVerify<TResponse>();
                    int readSize;
                    response = formatter.Deserialize(transaction.ResponseBuffer, 0, resolver, out readSize);
                } 

                handlers.Remove(sequence);
                reset.Set();
            });

            var headerBytes = MessagePackSerializer.Serialize(new RequestHeader { Command = command, Sequence = sequence } );

            byte[] instructionBytes = headerBytes;
            if(request != default(object))
            {
                instructionBytes = headerBytes.Concat(MessagePackSerializer.Serialize(request)).ToArray();
            }

            await _transportStream.WriteAsync(instructionBytes, 0, instructionBytes.Length);

            //reset.Wait(new TimeSpan(0,0,1), _cancellationTokenSource.Token);
            reset.Wait(_cancellationTokenSource.Token);

            return (response, error);
        }

        async Task<SerfError> HandleRequest<TRequest>(string command, TRequest request)
        {
            var sequence = (ulong)Interlocked.Increment(ref internalSeqence);

            var headerBytes = MessagePackSerializer.Serialize(new RequestHeader { Command = command, Sequence = sequence });
            var commandBytes = MessagePackSerializer.Serialize(request);
            var instructionBytes = headerBytes.Concat(commandBytes).ToArray();

            var cancellationToken = new CancellationTokenSource();
            SerfError error = null;
            var transaction = new TransactionContext { CancellationTokenSource = cancellationToken };
            handlers.Add(sequence, transaction);
            var reset = new ManualResetEventSlim(false);

            cancellationToken.Token.Register(() =>
            {
                if (!string.IsNullOrWhiteSpace(transaction.Header.Error))
                {
                    error = new SerfError { Error = transaction.Header.Error };
                }
                
                reset.Set();
            });

            await _transportStream.WriteAsync(instructionBytes, 0, instructionBytes.Length);

            reset.Wait(_cancellationTokenSource.Token);

            return error;
        }

        async Task ReadResponses(CancellationToken token)
        {
            try
            {
                using (_cancellationTokenSource.Token.Register(() => _transportStream.Close()))
                {
                    while (!token.IsCancellationRequested)
                    {
                        var read_buffer = new byte[8048];
                        var size = await _transportStream.ReadAsync(read_buffer, 0, read_buffer.Length, _cancellationTokenSource.Token);
                        if (size <= 0)
                        {
                            continue;
                        }

                        // Do not start to deserialize the request if the client is being terminated
                        if (token.IsCancellationRequested)
                        {
                            return;
                        }

                        var resolver = MessagePack.Resolvers.StandardResolver.Instance;
                        var formatter = resolver.GetFormatterWithVerify<ResponseHeader>();
                        int readSize;

                        try
                        {
                            var responseHeader = formatter.Deserialize(read_buffer, 0, resolver, out readSize);

                            if (handlers.ContainsKey(responseHeader.Seq))
                            {
                                var transaction = handlers[responseHeader.Seq];
                                transaction.Header = responseHeader;
                                transaction.ResponseBuffer = read_buffer.Skip(readSize).Take(size - readSize).ToArray();
                                transaction.CancellationTokenSource.Cancel();
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch
                        {
                            throw;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // If the receiving thread fails, all regsitered response handlers should be notified with an error
                var error = new ResponseHeader { Error = $"failed to read from socket: {e.Message} Message processing will be terminated." };
                foreach (var handler in handlers)
                {
                    handler.Value.Header = error;
                    handler.Value.CancellationTokenSource.Cancel();
                }

                throw;
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            try
            {
                _responseReaderTask.Wait();
            }
            catch (TaskCanceledException)
            {
                // Do not handle this exception!
            }
            catch
            {
                throw;
            }
        }
    }
}
