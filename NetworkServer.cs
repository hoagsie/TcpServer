using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Caching;
using System.Threading;

namespace Server
{
    public class NetworkServer : IDisposable
    {
        private readonly Func<MessageContext, MessageResponse> _messageHandler;

        private readonly ConcurrentDictionary<EndPoint, Socket> _clientSockets;
        private readonly ConcurrentDictionary<EndPoint, Guid> _clientSessions;
        private readonly ConcurrentDictionary<EndPoint, AvailabilityNotifier> _clientNotifiers;

        private static ConcurrentQueue<List<ArraySegment<byte>>> _BufferPool;

        private readonly Timer _poolTimer;
        private readonly Func<byte[], List<ArraySegment<byte>>> _bufferServer = GetBuffer;

        private int _timesPoolEmpty;

        private Socket _listenSocket;

        public event EventHandler<SocketErrorArgs> SocketErrorEncountered;
        public event EventHandler<MessageResponse> EndpointSocketNotFound;
        public event EventHandler<EndPoint> FailedAddingClient;
        public event EventHandler<EndPoint> FailedGeneratingSession;
        public event EventHandler<EndPoint> FailedWatchingClient;
        public event EventHandler<Guid> SessionClosed;
        public event EventHandler HighUsageAlert;

        private event EventHandler<Tuple<Socket, int>> CouldNotAddClient;
        private event EventHandler<Tuple<Socket, int>> CouldNotGenerateSession;
        private event EventHandler<Tuple<AvailabilityNotifier, int>> CouldNotWatchClient;

        private readonly MemoryCache _disposalCache;

        /// <summary>
        /// Instantiates a new instance of the <c>NetworkServer</c>.
        /// </summary>
        /// <param name="endpoint">The endpoint, address and port combination, over which the <c>NetworkServer</c> will communicate.</param>
        /// <param name="messageHandler">A <c>Func</c> object that will process incoming messages and be responsible for generating responses.</param>
        /// <param name="bufferPoolSize">The number of buffers that are available for socket operations.</param>
        /// <param name="bufferSize">The size of each buffer in the pool.</param>
        public NetworkServer(EndPoint endpoint, Func<MessageContext, MessageResponse> messageHandler, int bufferPoolSize, int bufferSize)
        {
            ConfigureListenSocket(endpoint);
            
            _messageHandler = messageHandler;

            _clientSockets = new ConcurrentDictionary<EndPoint, Socket>();
            _clientSessions = new ConcurrentDictionary<EndPoint, Guid>();
            _clientNotifiers = new ConcurrentDictionary<EndPoint, AvailabilityNotifier>();
            _BufferPool = new ConcurrentQueue<List<ArraySegment<byte>>>();

            BuildBufferPool(bufferPoolSize, bufferSize);

            _poolTimer = new Timer(BalanceBufferPool, new Tuple<int, int>(bufferPoolSize, bufferSize), TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(500));

            CouldNotAddClient += CouldNotAddClientHandler;
            CouldNotGenerateSession += CouldNotGenerateSessionHandler;
            CouldNotWatchClient += CouldNotWatchClientHandler;

            _disposalCache = new MemoryCache("Disposal");
        }

        private void BuildBufferPool(int bufferPoolSize, int bufferSize)
        {
            Enumerable.Range(1, bufferPoolSize).AsParallel()
                .ForAll(i =>
                {
                    _BufferPool.Enqueue(new List<ArraySegment<byte>> {new ArraySegment<byte>(new byte[bufferSize])});
                });
        }

        private void BalanceBufferPool(object state)
        {
            var tuple = (Tuple<int, int>) state;
            var poolSize = tuple.Item1;
            var bufferSize = tuple.Item2;

            if (_BufferPool.Count == 0)
            {
                Interlocked.Increment(ref _timesPoolEmpty);
            }

            if (_timesPoolEmpty > 3)
            {
                if (_BufferPool.Count > poolSize * 10)
                {
                    OnHighUsageAlert();
                    return;
                }

                BuildBufferPool(poolSize, bufferSize);
                Interlocked.Exchange(ref _timesPoolEmpty, 0);
            }

            while (_BufferPool.Count > poolSize)
            {
                _BufferPool.TryDequeue(out List<ArraySegment<byte>> list);
            }
        }

        private void ConfigureListenSocket(EndPoint endpoint)
        {
            _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.IP)
            {
                ExclusiveAddressUse = false,
                LingerState = new LingerOption(true, 5),
                NoDelay = true,
                Blocking = false
            };

            _listenSocket.Bind(endpoint);
        }

        public void Start()
        {
            _listenSocket.Listen(int.MaxValue);

            ThreadPool.QueueUserWorkItem(AcceptAsync);
        }

        private void AcceptAsync(object state)
        {
            var args = new SocketAsyncEventArgs();

            args.Completed += Accept_Completed;

            if (!_listenSocket.AcceptAsync(args))
            {
                CompleteAccept(args);
            }
        }

        private void CompleteAccept(SocketAsyncEventArgs args)
        {
            ThreadPool.QueueUserWorkItem(AcceptAsync);

            if (args.SocketError != SocketError.Success)
            {
                if (args.SocketError == SocketError.ConnectionReset)
                {
                    CleanUpClient(args.ConnectSocket);
                    return;
                }

                OnSocketErrorEncountered(new SocketErrorArgs
                {
                    SocketEndPoint = args.RemoteEndPoint,
                    Error = args.SocketError,
                    Source = SocketEventSource.Accept
                });
            }

            SetupNewClient(args);
        }

        private void SetupNewClient(SocketAsyncEventArgs args)
        {
            var socket = args.AcceptSocket;

            try
            {
                if (!_clientSockets.TryAdd(socket.RemoteEndPoint, socket))
                {
                    OnCouldNotAddClient(new Tuple<Socket, int>(socket, 1));
                }

                if (!_clientSessions.TryAdd(socket.RemoteEndPoint, Guid.NewGuid()))
                {
                    OnCouldNotGenerateSession(new Tuple<Socket, int>(socket, 1));
                }
            }
            catch (ArgumentNullException)
            {
                return;
            }

            var watcher = new AvailabilityNotifier(socket, ref _BufferPool, _bufferServer);

            watcher.DataAvailable += Client_DataAvailable;
            watcher.Disconnected += Client_Disconnected;

            if (_clientNotifiers.TryAdd(socket.RemoteEndPoint, watcher))
            {
                watcher.BeginWatching();
            }
            else
            {
                OnCouldNotWatchClient(new Tuple<AvailabilityNotifier, int>(watcher, 1));
            }
        }

        private void Client_Disconnected(object sender, Socket e)
        {
            ThreadPool.QueueUserWorkItem(CleanUpClient, e);
        }

        private void Client_DataAvailable(object sender, Socket e)
        {
            var buffer = GetBuffer();

            var args = new SocketAsyncEventArgs
            {
                BufferList = buffer,
                UserToken = e
            };
            
            args.Completed += Receive_Completed;

            try
            {
                if (!e.ReceiveAsync(args))
                {
                    CompleteReceive(args);
                }
            }
            catch (ObjectDisposedException)
            {
                
            }
        }

        private static List<ArraySegment<byte>> GetBuffer(byte[] prefillData = null)
        {
            List<ArraySegment<byte>> buffer;

            while (!_BufferPool.TryDequeue(out buffer))
            {
                Thread.Sleep(250);
            }

            var array = buffer.First().Array;

            if (array == null)
            {
                throw new InvalidOperationException("A fetched buffer has an internal array that was null.");
            }

            Array.Clear(array, 0, array.Length);

            if (prefillData != null)
            {
                if (prefillData.Length > array.Length)
                {
                    throw new InvalidOperationException("The prefill array contains more data than the fetched buffer was configured to allow.");
                }

                Array.Copy(prefillData, array, prefillData.Length);
            }

            return buffer;
        }

        private void CompleteReceive(SocketAsyncEventArgs args)
        {
            EndPoint endpoint;
            Socket socket;

            try
            {
                socket = (Socket) args.UserToken;
                endpoint = socket.RemoteEndPoint;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (args.SocketError != SocketError.Success)
            {
                if (args.SocketError == SocketError.ConnectionReset)
                {
                    CleanUpClient(args.UserToken);
                    return;
                }

                OnSocketErrorEncountered(new SocketErrorArgs
                {
                    SocketEndPoint = endpoint,
                    Error = args.SocketError,
                    Source = SocketEventSource.Receive
                });
            }

            if (args.BytesTransferred == 0)
            {
                CleanUpClient(socket);
                return;
            }

            _clientSessions.TryGetValue(endpoint, out Guid sessionId);

            var receiveBuffer = new byte[args.BytesTransferred];
            Array.Copy(args.BufferList.First().Array, receiveBuffer, receiveBuffer.Length);

            _BufferPool.Enqueue(args.BufferList.ToList());

            _messageHandler.BeginInvoke(
                new MessageContext
                {
                    SessionId = sessionId,
                    ReceivedOn = endpoint,
                    Request = receiveBuffer
                }, MessageHandlerComplete, _messageHandler);
        }

        private void MessageHandlerComplete(IAsyncResult ar)
        {
            var handler = (Func<MessageContext, MessageResponse>)ar.AsyncState;

            var response = handler.EndInvoke(ar);

            ThreadPool.QueueUserWorkItem(SendToSocket, response);
        }

        private void SendToSocket(object state)
        {
            var response = (MessageResponse)state;

            _clientSockets.TryGetValue(response.Context.ReceivedOn, out Socket client);

            if (client == null)
            {
                OnEndpointSocketNotFound(response);
                return;
            }

            var buffer = new List<ArraySegment<byte>> {new ArraySegment<byte>(response.Body)};

            var args = new SocketAsyncEventArgs
            {
                BufferList = buffer,
                UserToken = client
            };

            args.Completed += Send_Completed;

            try
            {
                if (!client.SendAsync(args))
                {
                    CompleteSend(args);
                }
            }
            catch (ObjectDisposedException)
            {
                
            }
        }

        private void CompleteSend(SocketAsyncEventArgs args)
        {
            if (args.SocketError != SocketError.Success)
            {
                var socket = (Socket) args.UserToken;

                if (args.SocketError == SocketError.ConnectionAborted || args.SocketError == SocketError.ConnectionReset || args.SocketError == SocketError.Shutdown)
                {
                    CleanUpClient(socket);
                    return;
                }

                OnSocketErrorEncountered(new SocketErrorArgs
                {
                    SocketEndPoint = socket.RemoteEndPoint,
                    Error = args.SocketError,
                    Source = SocketEventSource.Send
                });
            }

            _BufferPool.Enqueue(args.BufferList.ToList());
        }

        private void Send_Completed(object sender, SocketAsyncEventArgs e)
        {
            CompleteSend(e);
        }

        private void CleanUpClient(object state)
        {
            var stateSocket = (Socket) state;

            if (stateSocket == null || _disposalCache.Contains(stateSocket.Handle.ToString()))
            {
                return;
            }

            try
            {
                var endpoint = stateSocket.RemoteEndPoint;

                _clientNotifiers.TryRemove(endpoint, out AvailabilityNotifier notifier);
                _clientSockets.TryRemove(endpoint, out Socket socket);
                _clientSessions.TryRemove(endpoint, out Guid sessionId);
                
                socket.Shutdown(SocketShutdown.Both);
                socket.Close(5);

                notifier.Dispose();

                OnSessionClosed(sessionId);
            }
            catch
            {
                // ignored
            }
            finally
            {
                var cacheItem = new CacheItem(stateSocket.Handle.ToString(), stateSocket);
                var cachePolicy = new CacheItemPolicy {AbsoluteExpiration = DateTimeOffset.Now.AddSeconds(1)};

                _disposalCache.Add(cacheItem, cachePolicy);
            }
        }

        private void Receive_Completed(object sender, SocketAsyncEventArgs e)
        {
            CompleteReceive(e);
        }

        private void Accept_Completed(object sender, SocketAsyncEventArgs e)
        {
            CompleteAccept(e);
        }

        private void CouldNotAddClientHandler(object sender, Tuple<Socket, int> e)
        {
            var socket = e.Item1;
            var attempts = e.Item2;

            if (attempts >= 3)
            {
                OnFailedAddingClient(socket.RemoteEndPoint);
                return;
            }

            if (!_clientSockets.TryAdd(socket.RemoteEndPoint, socket))
            {
                OnCouldNotAddClient(new Tuple<Socket, int>(socket, attempts + 1));
            }
        }

        private void CouldNotGenerateSessionHandler(object sender, Tuple<Socket, int> e)
        {
            var socket = e.Item1;
            var attempts = e.Item2;

            if (attempts >= 3)
            {
                OnFailedGeneratingSession(socket.RemoteEndPoint);
                return;
            }

            if (!_clientSessions.TryAdd(socket.RemoteEndPoint, Guid.NewGuid()))
            {
                OnCouldNotGenerateSession(new Tuple<Socket, int>(socket, attempts + 1));
            }
        }

        private void CouldNotWatchClientHandler(object sender, Tuple<AvailabilityNotifier, int> e)
        {
            var watcher = e.Item1;
            var attempts = e.Item2;

            if (attempts >= 3)
            {
                OnFailedWatchingClient(watcher.Target.RemoteEndPoint);
                return;
            }

            if (_clientNotifiers.TryAdd(watcher.Target.RemoteEndPoint, watcher))
            {
                watcher.BeginWatching();
            }
            else
            {
                OnCouldNotWatchClient(new Tuple<AvailabilityNotifier, int>(watcher, attempts + 1));
            }
        }

        protected virtual void OnSocketErrorEncountered(SocketErrorArgs e)
        {
            SocketErrorEncountered?.BeginInvoke(this, e, EndInvokeSocketError, SocketErrorEncountered);
        }

        private void EndInvokeSocketError(IAsyncResult ar)
        {
            var socketError = (EventHandler<SocketErrorArgs>) ar.AsyncState;
            socketError.EndInvoke(ar);
        }

        protected virtual void OnEndpointSocketNotFound(MessageResponse e)
        {
            EndpointSocketNotFound?.BeginInvoke(this, e, EndInvokeMessageResponse, EndpointSocketNotFound);
        }

        private void EndInvokeMessageResponse(IAsyncResult ar)
        {
            var messageResponse = (EventHandler<MessageResponse>) ar.AsyncState;
            messageResponse.EndInvoke(ar);
        }

        protected virtual void OnCouldNotAddClient(Tuple<Socket, int> e)
        {
            CouldNotAddClient?.BeginInvoke(this, e, EndInvokeSocketInt, CouldNotAddClient);
        }

        private void EndInvokeSocketInt(IAsyncResult ar)
        {
            var socketInt = (EventHandler<Tuple<Socket, int>>) ar.AsyncState;
            socketInt.EndInvoke(ar);
        }

        protected virtual void OnFailedAddingClient(EndPoint e)
        {
            FailedAddingClient?.BeginInvoke(this, e, EndInvokeEndpoint, FailedAddingClient);
        }

        private void EndInvokeEndpoint(IAsyncResult ar)
        {
            var endpoint = (EventHandler<EndPoint>) ar.AsyncState;
            endpoint.EndInvoke(ar);
        }

        protected virtual void OnCouldNotGenerateSession(Tuple<Socket, int> e)
        {
            CouldNotGenerateSession?.BeginInvoke(this, e, EndInvokeSocketInt, CouldNotGenerateSession);
        }

        protected virtual void OnFailedGeneratingSession(EndPoint e)
        {
            FailedGeneratingSession?.BeginInvoke(this, e, EndInvokeEndpoint, FailedGeneratingSession);
        }

        protected virtual void OnCouldNotWatchClient(Tuple<AvailabilityNotifier, int> e)
        {
            CouldNotWatchClient?.BeginInvoke(this, e, EndInvokeNotifierInt, CouldNotWatchClient);
        }

        private void EndInvokeNotifierInt(IAsyncResult ar)
        {
            var notifierInt = (EventHandler<Tuple<AvailabilityNotifier, int>>) ar.AsyncState;
            notifierInt.EndInvoke(ar);
        }

        protected virtual void OnFailedWatchingClient(EndPoint e)
        {
            FailedWatchingClient?.BeginInvoke(this, e, EndInvokeEndpoint, FailedWatchingClient);
        }

        protected virtual void OnSessionClosed(Guid e)
        {
            SessionClosed?.BeginInvoke(this, e, EndInvokeGuid, SessionClosed);
        }

        private void EndInvokeGuid(IAsyncResult ar)
        {
            var guid = (EventHandler<Guid>) ar.AsyncState;
            guid.EndInvoke(ar);
        }

        protected virtual void OnHighUsageAlert()
        {
            HighUsageAlert?.BeginInvoke(this, EventArgs.Empty, EndInvoke, HighUsageAlert);
        }

        private void EndInvoke(IAsyncResult ar)
        {
            var handler = (EventHandler)ar.AsyncState;
            handler.EndInvoke(ar);
        }

        public void Dispose()
        {
            _listenSocket.Shutdown(SocketShutdown.Both);
            _listenSocket.Close(1);
            _listenSocket?.Dispose();

            _disposalCache.Dispose();
            _poolTimer.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}
