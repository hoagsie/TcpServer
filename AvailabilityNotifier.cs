using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;

namespace Server
{
    public class AvailabilityNotifier : IDisposable
    {
        private readonly ConcurrentQueue<List<ArraySegment<byte>>> _bufferPool;
        private readonly Func<byte[], List<ArraySegment<byte>>> _bufferProvider;
        private readonly Timer _checkTimer;

        public Socket Target { get; private set; }

        public event EventHandler<Socket> DataAvailable;
        public event EventHandler<Socket> Disconnected;

        public AvailabilityNotifier(Socket socket, ref ConcurrentQueue<List<ArraySegment<byte>>> bufferPool, Func<byte[], List<ArraySegment<byte>>> bufferProvider)
        {
            Target = socket;

            _bufferPool = bufferPool;
            _bufferProvider = bufferProvider;
            _checkTimer = new Timer(CheckAvaiable);
        }

        public void BeginWatching()
        {
            _checkTimer.Change(TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(200));
        }

        private void CheckAvaiable(object state)
        {
            try
            {
                if (Target.Available > 0)
                {
                    OnDataAvailable();
                }

                var buffer = _bufferProvider(null);

                var args = new SocketAsyncEventArgs
                {
                    BufferList = buffer,
                    SocketFlags = SocketFlags.Peek
                };
                
                args.Completed += Args_Completed;

                if (!Target.ReceiveAsync(args))
                {
                    CompletePeek(args);
                }
            }
            catch (ObjectDisposedException)
            {

            }
            catch (NullReferenceException)
            {
                
            }
        }

        private void Args_Completed(object sender, SocketAsyncEventArgs e)
        {
            CompletePeek(e);
        }

        private void CompletePeek(SocketAsyncEventArgs args)
        {
            if (args.BytesTransferred == 0)
            {
                OnDisconnected();
            }

            _bufferPool.Enqueue(args.BufferList.ToList());
        }

        protected virtual void OnDataAvailable()
        {
            DataAvailable?.BeginInvoke(this, Target, EndEventInvoke, DataAvailable);
        }

        private void EndEventInvoke(IAsyncResult ar)
        {
            ((EventHandler<Socket>)ar.AsyncState).EndInvoke(ar);
        }

        public void Dispose()
        {
            Target?.Dispose();
            _checkTimer?.Dispose();

            GC.SuppressFinalize(this);
        }

        protected virtual void OnDisconnected()
        {
            Disconnected?.BeginInvoke(this, Target, EndEventInvoke, Disconnected);
        }
    }
}
