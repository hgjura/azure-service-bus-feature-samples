using Serilog;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using ASB.Common.Core;

namespace ASB.Subscriber.Core
{
    public static class Receive
    {


        #region Topics

        public static async Task ListenToTopicSubscriptionsAsync(string ConnectionString, string TopicName, List<string> Subscriptions)
        {
            var cts = new CancellationTokenSource();
            List<Task> subs = new List<Task>();

            Subscriptions.ForEach((s) => subs.Add(ReceiveMessages_FromTopicSubscriptionsAsync(new SubscriptionClient(ConnectionString, TopicName, s), cts.Token, ConsoleColor.Cyan)));

            var allListeneres = Task.WhenAll(subs);

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(()=>Console.ReadKey()), 
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith(
                    (t)=> cts.Cancel()),
                    allListeneres
                );
        }
        private static async Task ReceiveMessages_FromTopicSubscriptionsAsync(SubscriptionClient client, CancellationToken token, ConsoleColor color)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(
                            async () => {
                                await client.CloseAsync();
                                doneReceiving.SetResult(true);}
                          );

            client.RegisterMessageHandler(
                async (message, token1) =>
                {
                    try
                    {
                        if (ProcessMessages(message, ConsoleColor.White))
                        {
                            await client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message is of the wrong type", "Cannot deserialize this message as the type in the Label prop is unknown.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"[{message.MessageId}] -- {ex.Message}");
                    }
                },                 
                GetDefaultMessageHandlingOptions());

            await doneReceiving.Task;
        }

        #endregion

        #region Dead-lettering

        public static async Task ListenToTopicDeadLetterSubscriptionsAsync(string ConnectionString, string TopicName, string SubscriptionName)
        {
            var cts = new CancellationTokenSource();
            
            var allListeneres = Task.WhenAll(
                ReceiveMessages_FromTopicDeadLetterSubscriptions(
                    new MessageSender(ConnectionString, TopicName),
                    new SubscriptionClient(ConnectionString, TopicName, EntityNameHelper.FormatDeadLetterPath(SubscriptionName), ReceiveMode.PeekLock), 
                    cts.Token, 
                    ConsoleColor.Cyan)
                );


            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith((t) => cts.Cancel()),
                allListeneres
                );
        }
        private static async Task ReceiveMessages_FromTopicDeadLetterSubscriptions(MessageSender sender, SubscriptionClient client, CancellationToken token, ConsoleColor color)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(
                async () =>
                {
                    await client.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            client.RegisterMessageHandler(
                async (message, token1) =>
                {
                    try
                    {
                        var resubmitMessage = message.Clone();

                        resubmitMessage.MessageId = $"{resubmitMessage.MessageId}|{Guid.NewGuid()}"; 

                        await sender.SendAsync(resubmitMessage);
                        await client.CompleteAsync(message.SystemProperties.LockToken);

                        Utils.ConsoleWrite($"[{message.MessageId}] -- Placed back in topic.\n", ConsoleColor.Blue, color);
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"[{message.MessageId}] -- {ex.Message}");
                        await client.AbandonAsync(message.SystemProperties.LockToken);
                    }
                },
                GetDefaultMessageHandlingOptions());

            await doneReceiving.Task;
        }

        #endregion

        #region PrioritySubscription

        public static async Task ListenToTopicSubscriptionsWithPriorityAsync(string ConnectionString, string TopicName, List<string> Subscriptions)
        {
            var cts = new CancellationTokenSource();
            List<Task> subs = new List<Task>();

            Subscriptions.ForEach((s) => subs.Add(ReceiveMessages_FromTopicSubscriptionsWithPriorityAsync(new SubscriptionClient(ConnectionString, TopicName, s), cts.Token, ConsoleColor.Cyan)));

            var allListeneres = Task.WhenAll(subs);

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith(
                    (t) => cts.Cancel()),
                    allListeneres
                );
        }
        private static async Task ReceiveMessages_FromTopicSubscriptionsWithPriorityAsync(SubscriptionClient client, CancellationToken token, ConsoleColor color)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(async () =>  
            {
                await client.CloseAsync();
                doneReceiving.SetResult(true);
            });

            client.RegisterMessageHandler(
                async (message, token1) =>
                {
                    try
                    {
                        if (ProcessMessages(message, color))
                        {
                            await client.CompleteAsync(message.SystemProperties.LockToken);
                        }
                        else
                        {
                            await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message is of the wrong type", "Cannot deserialize this message as the type in the Label prop is unknown.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Unable to receive {0} from subscription: Exception {1}", message.MessageId, ex);
                    }
                },
                GetDefaultMessageHandlingOptions());

            await doneReceiving.Task;
        }

        #endregion

        #region Message browsing

        public static async Task ListenToTopicSubscriptionsBrowsingAsync(string ConnectionString, string TopicName, string SubscriptionName)
        {
            var receiver = new MessageReceiver(ConnectionString, EntityNameHelper.FormatSubscriptionPath(TopicName, SubscriptionName), ReceiveMode.PeekLock, null, 0);

            while (true)
            {
                try
                {
                    // Browse messages from queue
                    var message = await receiver.PeekAsync();
                    
                    // If the returned message value is null, we have reached the bottom of the log
                    if (message != null)
                    {
                        ProcessMessages(message, ConsoleColor.White);
                    }
                    else
                    {
                        // We have reached the end of the log.
                        break;
                    }
                }
                catch (ServiceBusException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                }
            }
            await receiver.CloseAsync();
        }

        #endregion

        #region Message prefetch

        public static async Task ListenToTopicSubscriptionsPrefetchAsync(string ConnectionString, string TopicName, string SubscriptionName)
        {
            var receiver = new MessageReceiver(ConnectionString, EntityNameHelper.FormatSubscriptionPath(TopicName, SubscriptionName), ReceiveMode.ReceiveAndDelete, null, 0);
            receiver.PrefetchCount = 100;

            while (true)
            {
                try
                {
                    // Browse messages from queue
                    var message = await receiver.ReceiveAsync();

                    // If the returned message value is null, we have reached the bottom of the log
                    if (message != null)
                    {
                        ProcessMessages(message, ConsoleColor.White);
                    }
                    else
                    {
                        // We have reached the end of the log.
                        break;
                    }
                }
                catch (ServiceBusException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                }
            }
            await receiver.CloseAsync();
        }
        #endregion

        #region Session Correlation & SessionState

        public static async Task ListenToTopicSessionSubscriptionsAsync(string ConnectionString, string TopicName, string SubscriptionName, int EndCountForSession)
        {
            var cts = new CancellationTokenSource();

            var allListeneres = Task.WhenAll(
                ReceiveMessages_FromTopicSessionSubscriptionsAsync(new SubscriptionClient(ConnectionString, TopicName, SubscriptionName), cts.Token, ConsoleColor.Green, EndCountForSession)
            );

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith((t) => cts.Cancel()),
                allListeneres
                );
        }
        private static async Task ReceiveMessages_FromTopicSessionSubscriptionsAsync(SubscriptionClient client, CancellationToken token, ConsoleColor color, int EndCount)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(
                async () =>
                {
                    await client.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            client.RegisterSessionHandler(
                async (session, message, token1) =>
                {
                    try
                    {
                        Utils.ConsoleWrite($"[{message.SessionId}]\n", ConsoleColor.Blue, ConsoleColor.Red);
                        
                        if (ProcessMessages(message, ConsoleColor.White))
                        {
                            await session.CompleteAsync(message.SystemProperties.LockToken);
                            if(((int)message.UserProperties["Count"]) == EndCount)
                            {
                                //end of the session
                                await session.CloseAsync();
                            }
                        }
                        else
                        {
                            await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message is of the wrong type", "Cannot deserialize this message as the type in the Label prop is unknown.");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Unable to receive {0} from subscription: Exception {1}", message.MessageId, ex);
                    }
                },
                GetDefaultSessionHandlingOptions());

            await doneReceiving.Task;
        }



        public static async Task ListenToTopicSessionStateSubscriptionsAsync(string ConnectionString, string TopicName, string SubscriptionName, int EndCountForSession)
        {
            var cts = new CancellationTokenSource();

            var allListeneres = Task.WhenAll(
                ReceiveMessages_FromTopicSessionStateSubscriptionsAsync(new SubscriptionClient(ConnectionString, TopicName, SubscriptionName), cts.Token, ConsoleColor.Green, EndCountForSession)
            );

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromMinutes(5))
                ).ContinueWith((t) => cts.Cancel()),
                allListeneres
                );
        }
        private static async Task ReceiveMessages_FromTopicSessionStateSubscriptionsAsync(SubscriptionClient client, CancellationToken token, ConsoleColor color, int EndCount)
        {
            var doneReceiving = new TaskCompletionSource<bool>();

            token.Register(
                async () =>
                {
                    await client.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            client.RegisterSessionHandler(
                async (session, message, token1) =>
                {
                    try
                    {
                        var stateData = await session.GetStateAsync();

                        var session_state = stateData != null ? Utils.Deserialize<SessionStateManager>(stateData) : new SessionStateManager();

                        if ((int)message.UserProperties["Count"] == session_state.LastProcessedCount + 1)  //check if message is next in the sequence
                        {
                            if (ProcessMessages(message, ConsoleColor.White))
                            {
                                await session.CompleteAsync(message.SystemProperties.LockToken);
                                if (((int)message.UserProperties["Count"]) == EndCount)
                                {
                                    //end of the session
                                    await session.CloseAsync();
                                }
                            }
                            else
                            {
                                await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message is of the wrong type", "Cannot deserialize this message as the type in the Labl prop is uknown.");
                            }
                        }
                        else
                        {
                            session_state.DeferredList.Add((int)message.UserProperties["Count"], message.SystemProperties.SequenceNumber);
                            await session.DeferAsync(message.SystemProperties.LockToken);
                            await session.SetStateAsync(Utils.Serialize<SessionStateManager>(session_state));
                        }
                        
                        long last_processed = await ProcessNextMessagesWithSessionStateAsync(client, session, session_state, EndCount, color);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Unable to receive {0} from subscription: Exception {1}", message.MessageId, ex);
                    }
                },
                GetDefaultSessionHandlingOptions());

            await doneReceiving.Task;
        }

        #endregion

        #region Helper functions
        private static bool ProcessMessages(Message message, ConsoleColor color)
        {
            if (message.Label == "Stock")
            {
                var stock = Utils.Deserialize<Stock>(message.Body);

                var s = $"\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {message.MessageId}, \n\t\t\t\t\t\tSequenceNumber = {message.SystemProperties.SequenceNumber}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {message.SystemProperties.EnqueuedTimeUtc},\n\t\t\t\t\t\tContent: {stock}";

                Utils.ConsoleWrite(s, ConsoleColor.Blue, color);

                return true;
            }

            return false;
        }

        private static async Task<long> ProcessNextMessagesWithSessionStateAsync(SubscriptionClient client, IMessageSession session, SessionStateManager session_state, int EndOfSessionCount, ConsoleColor color)
        {
            int x = session_state.LastProcessedCount + 1;

            long seq2 = 0;

            if (session_state.DeferredList.TryGetValue(x, out long seq1))

                while (true)
                {

                    if (!session_state.DeferredList.TryGetValue(x, out seq2)) break;

                    //-------------------------------
                    var deferredMessage = await session.ReceiveDeferredMessageAsync(seq2);

                    if (ProcessMessages(deferredMessage, ConsoleColor.White))
                    {
                        await session.CompleteAsync(deferredMessage.SystemProperties.LockToken);
                        session_state.LastProcessedCount = ((int)deferredMessage.UserProperties["Count"]);
                        session_state.DeferredList.Remove(x);
                        await session.SetStateAsync(Utils.Serialize<SessionStateManager>(session_state));
                    }
                    else
                    {
                        await client.DeadLetterAsync(deferredMessage.SystemProperties.LockToken, "Message is of the wrong type", "Cannot deserialize this message as the type in the Label prop is unknown.");
                        session_state.DeferredList.Remove(x);
                        await session.SetStateAsync(Utils.Serialize<SessionStateManager>(session_state));
                    }

                    //------------------------------

                    x++;

                }

            return seq2;            
        }
        
        private static MessageHandlerOptions GetDefaultMessageHandlingOptions()
        {
            return new MessageHandlerOptions(e => LogMessageHandlerException(e, new LoggerConfiguration().WriteTo.ColoredConsole().CreateLogger()))
            {
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromMinutes(1),
                MaxConcurrentCalls = 1000
            };
        }

        private static SessionHandlerOptions GetDefaultSessionHandlingOptions()
        {
            return new SessionHandlerOptions(e => LogMessageHandlerException(e, new LoggerConfiguration().WriteTo.ColoredConsole().CreateLogger()))
            {
                MessageWaitTimeout = TimeSpan.FromSeconds(5),
                MaxConcurrentSessions = 1,
                AutoComplete = false
            };
        }

        private static Task LogMessageHandlerException(ExceptionReceivedEventArgs e, ILogger l)
        {
            l.Fatal($"\n Error: [{e.ExceptionReceivedContext.EntityPath}] -- {e.Exception.Message}.");
            return Task.CompletedTask;
        }

        #endregion
    }
}
