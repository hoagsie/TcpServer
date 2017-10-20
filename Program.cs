using System;
using System.Diagnostics;
using System.Net;
using System.ServiceModel;
using System.Text;
using NewTcpServer.ServiceReference1;
using Server;

namespace NewTcpServer
{
    class Program
    {
        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            var server = new NetworkServer(new IPEndPoint(IPAddress.Parse("10.12.48.102"), 9001), MessageHandler, 100, 4096);

            server.EndpointSocketNotFound += Server_EndpointSocketNotFound;
            server.FailedAddingClient += Server_FailedAddingClient;
            server.SocketErrorEncountered += Server_SocketErrorEncountered;
            server.FailedGeneratingSession += Server_FailedGeneratingSession;
            server.SessionClosed += Server_SessionClosed;
            server.HighUsageAlert += Server_HighUsageAlert;
            
            server.Start();

            Console.WriteLine("Started");

            Console.ReadLine();
        }

        private static void Server_HighUsageAlert(object sender, EventArgs e)
        {
            Console.WriteLine("High usage detected. Performance degradation likely.");
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var error = e.ExceptionObject as Exception;

            Console.WriteLine(error);
        }

        private static void Server_SessionClosed(object sender, Guid e)
        {
            //Console.WriteLine(e);
        }

        private static void Server_FailedGeneratingSession(object sender, EndPoint e)
        {
            Console.WriteLine("Failed generating session for " + e);
        }

        private static void Server_SocketErrorEncountered(object sender, SocketErrorArgs e)
        {
            Console.WriteLine("SOCKET ERROR");
            Console.WriteLine("Who? " + e.SocketEndPoint);
            Console.WriteLine("What? " + e.Error);
            Console.WriteLine("When? " + e.Source);
        }

        private static void Server_FailedAddingClient(object sender, EndPoint e)
        {
            Console.WriteLine("Failed adding " + e);
        }

        private static void Server_EndpointSocketNotFound(object sender, MessageResponse e)
        {
            Console.WriteLine("Could not respond to " + e.Context.ReceivedOn);
        }

        private static Service1Client _FloatingClient;
        
        private static Service1Client FloatingClient
        {
            get
            {
                _FloatingClient = _FloatingClient ?? new Service1Client();

                if (_FloatingClient.State != CommunicationState.Faulted)
                {
                    return _FloatingClient;
                }

                try
                {
                    _FloatingClient.Close();
                }
                catch
                {
                    _FloatingClient.Abort();
                }

                ((IDisposable)_FloatingClient).Dispose();

                _FloatingClient = new Service1Client();

                return _FloatingClient;
            }
        }

        private static MessageResponse MessageHandler(MessageContext messageContext)
        {
            var request = Encoding.Default.GetString(messageContext.Request);

            try
            {
                string response = FloatingClient.GetData(Convert.ToInt32(request));

                return MessageResponse.Create(messageContext, Encoding.Default.GetBytes(response));
            }
            catch (Exception error)
            {
                Console.WriteLine("Request - " + request);
                Console.WriteLine(error);
                return MessageResponse.Create(messageContext, Encoding.Default.GetBytes(error.Message));
            }
        }
    }
}
