using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ThreadState = System.Threading.ThreadState;

namespace Client
{
    class Program
    {
        private static Random rand;

        private static CancellationTokenSource cts;

        private static List<Thread[]> Pools = new List<Thread[]>();

        private static ConcurrentBag<long> WatchBag = new ConcurrentBag<long>();

        private static int sent;
        private static int open;

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            var timer = new Timer(StatusCheck, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

            rand = new Random();
            cts = new CancellationTokenSource();

            cts.Token.Register(CancelThreads);

            foreach (var i in Enumerable.Range(0, 10))
            {
                DoTestBatch();
            }

            while (open > 1)
            {
                Thread.Sleep(250);
            }
            
            Console.WriteLine("Done " + sent);
            timer.Change(TimeSpan.FromMilliseconds(-1), TimeSpan.FromMilliseconds(-1));
            Console.WriteLine("Avg Chat " + WatchBag.Average());
            Console.WriteLine("Min " + WatchBag.Min());
            Console.WriteLine("Max " + WatchBag.Max());
            Console.ReadLine();
        }

        private static void StatusCheck(object state)
        {
            Console.WriteLine("Open " + open);
            Console.WriteLine("Sent " + sent);
        }

        private static void CancelThreads()
        {
            Pools.AsParallel().ForAll(threads =>
            {
                threads.AsParallel().ForAll(thread =>
                {
                    if (thread != null && (thread.ThreadState != ThreadState.Stopped || thread.ThreadState != ThreadState.Unstarted))
                    {
                        thread.Abort();
                    }
                });
            });
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var exception = e.ExceptionObject as Exception;

            Console.WriteLine(exception);

            cts.Cancel();
        }

        private static void DoTestBatch()
        {
            var thread = new Thread[100];

            Pools.Add(thread);

            for (var i = 0; i < thread.Length; i++)
            {
                thread[i] = new Thread(ConnectAndTalk);
            }

            thread.AsParallel().ForAll(thread1 =>
            {
                while (open > 100)
                {
                    Thread.Sleep(250);
                }

                if (!cts.IsCancellationRequested)
                {
                    thread1.Start();
                }
            });
        }

        private static void ConnectAndTalk(object i)
        {
            var client = new TcpClient();

            InsistConnect(client);

            Interlocked.Increment(ref open);

            foreach (var i1 in Enumerable.Range(0, 100))
            {
                Chat(client, i1);
            }

            client.Close();
            client.Client?.Dispose();

            Interlocked.Decrement(ref open);
        }

        private static void InsistConnect(TcpClient client)
        {
            try
            {
                while (open > 100)
                {
                    Thread.Sleep(250);
                }

                client.Connect(IPAddress.Parse("10.12.48.102"), 9001);
            }
            catch (SocketException)
            {
                Thread.Sleep(250);

                InsistConnect(client);
            }
        }

        private static void Chat(TcpClient client, int i1)
        {
            var watch = Stopwatch.StartNew();
            var stream = client.GetStream();

            //var prefix = rand.Next(1, 4) % 2 == 0 ? (char) 2 : (char) 5;

            var message = i1.ToString();
            var buffer = Encoding.Default.GetBytes(message);

            stream.Write(buffer, 0, buffer.Length);

            buffer = new byte[4096];
            var read = stream.Read(buffer, 0, buffer.Length);
            message = Encoding.Default.GetString(buffer, 0, read);

            WatchBag.Add(watch.ElapsedTicks);
            Interlocked.Increment(ref sent);

            Task.Run(() => Console.WriteLine(message));
        }
    }
}
