using Microsoft.Azure.ServiceBus;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Core;
using ASB.Common.Core;

namespace ASB.Publisher.Core
{
    public static class Publish
    {

        //Duplicate detection [set flag in SB topic in SB Explorer]
        //Autoforward [set flag in SB topic in SB Explorer]
        //Batching [ok]
        //Deferral [ok (check Session)]
        //Durable sender [explain only]
        //Geo replication [ok]
        //Partitioning [ok]
        //Creating priority subscriptions [ok]
        //Sessions [ok]
        //SessionStates [ok]
        //ScheduledMessage [ok]
        //Correlation (explain multiple corr concepts) [ok]
        //Message browsing [ok]
        //Transactions (explain only)
        //Archiving [ok]

        #region Topics/Subscriptions

        public static Task PublishToTopic(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send messages async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));

            return Task.WhenAll(all_tasks.ToArray());
        }
        public static Task PublishHighVolumeToTopic(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();
            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());
        }
        public static Task PublishToTopicInBatch(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(100, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send async
            var sender = new MessageSender(ConnectionString, TopicName);
            return sender.SendAsync(all_messages);

        }

        #endregion

        #region Dead-lettering

        public static Task PublishToTopicWithDeadlettering(string ConnectionString, string TopicName)
        {
            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromSeconds(1),
                    SessionId = $"{m.Name}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));

            return Task.WhenAll(all_tasks.ToArray());

        }

        #endregion

        #region Deferral

        //check DeferAsync on how it is used in session state examples

        #endregion

        #region Geo-rep

        //active geo-replication

        public static async Task PublishToTopicWithActiveGeoreplication(string ConnectionString, string TopicName)
        {
            var primary_sender = new MessageSender(ConnectionString, TopicName);
            var secondary_sender = new MessageSender(ConnectionString, TopicName);

            var list = new List<Message>();

            for (var i = 1; i <= 5; i++)
            {
                // Create brokered message.
                var m1 = new Message(Encoding.UTF8.GetBytes("Message" + i))
                {
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2.0)
                };

                list.Add(m1);
            }


            foreach (var m in list)
            {
                var ex_count = 0;
                var backup_message = m.Clone();

                //var t1 = primary_sender.SendAsync(m);
                //var t2 = secondary_sender.SendAsync(backup_message);

                try
                {
                    //t1.Wait();
                    await primary_sender.SendAsync(m);
                    Log.Information("Message {0} sent to primary queue: Body = {1}", m.MessageId, Encoding.UTF8.GetString(m.Body));
                }
                catch (Exception ex)
                {
                    Log.Error("Unable to send message {0} to primary queue: Exception {1}", m.MessageId, ex);
                    ex_count++;
                }

                try
                {
                    await secondary_sender.SendAsync(backup_message);
                    //t2.Wait();
                    Log.Information("Message {0} sent to secondary queue: Body = {1}", backup_message.MessageId, Encoding.UTF8.GetString(backup_message.Body));
                }
                catch (Exception ex)
                {
                    Log.Error("Unable to send message {0} to secondary queue: Exception {1}", backup_message.MessageId, ex);
                    ex_count++;
                }

                if (ex_count > 1)
                {
                    throw new Exception("Send Failure");
                }

            }
        }

        //passive geo-replication
        public static async Task PublishToTopicWithPassiveGeoreplication(string ConnectionString, string TopicName)
        {

            var primary_sender = new MessageSender(ConnectionString, TopicName);
            var secondary_sender = new MessageSender(ConnectionString, TopicName);

            var list = new List<Message>();

            for (var i = 1; i <= 5; i++)
            {
                // Create brokered message.
                var m1 = new Message(Encoding.UTF8.GetBytes("Message" + i))
                {
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2.0)
                };

                list.Add(m1);
            }


            foreach (var m in list)
            {
                try
                {
                    await PublishMessageToPrimaryOrSecondaryTopicAsync(m, primary_sender, secondary_sender, 10);
                    Log.Information("Message {0} sent to primary or secondary queue: Body = {1}", m.MessageId, Encoding.UTF8.GetString(m.Body));
                }
                catch (Exception ex)
                {
                    Log.Error("Unable to send message {0} to primary or secondary queue: Exception {1}", m.MessageId, ex);
                }

            }
        }

        static readonly object swapMutex = new object();
        private static async Task PublishMessageToPrimaryOrSecondaryTopicAsync(Message m1, MessageSender primary, MessageSender secondary, int MaxSendRetries = 10)
        {

            do
            {
                var m2 = m1.Clone();
                try
                {
                    await primary.SendAsync(m1);
                    return;
                }
                catch
                {
                    if (--MaxSendRetries <= 0)
                    {
                        throw;
                    }

                    lock (swapMutex)
                    {
                        var c = primary;
                        primary = secondary;
                        secondary = c;
                    }
                    m1 = m2.Clone();
                }
            }
            while (true);
        }



        #endregion

        #region Partitioning

        public static Task PublishToTopicWithPartitioning(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    //SessionId = $"{m.Name}",

                    PartitionKey = $"{m.Name}".Substring(0, 1)
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());
        }

        #endregion

        #region Priority Subscriptions

        public static Task PublishToTopicWithPriority(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    //SessionId = $"{m.Name}",

                    PartitionKey = $"{m.Name}".Substring(0, 1)
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);
                message.UserProperties.Add("Priority", new Random().Next(1, 4));

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());
        }

        #endregion

        #region Scheduled messages

        public static Task PublishToTopicWithScheduledMessages(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}",

                    ScheduledEnqueueTimeUtc = DateTime.UtcNow + TimeSpan.FromSeconds(30)

                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);
                message.UserProperties.Add("Priority", new Random().Next(1, 4));

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());
        }



        #endregion

        #region Message browsing with MessageSender

        public static Task PublishToTopicWithMessageBrowsing(string ConnectionString, string TopicName)
        {

            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}",
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);
                message.UserProperties.Add("Priority", new Random().Next(1, 4));

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());

        }

        #endregion

        #region Message Correlation 

        public static Task PublishToTopicWithMessageCorrelation(string ConnectionString, string TopicName)
        {
            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}",
                    CorrelationId = m.Name.Contains(".TO") ? "Canada" : "USA"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);
            all_messages.ForEach(m => all_tasks.Add(sender.SendAsync(m)));
            return Task.WhenAll(all_tasks.ToArray());
        }


        #endregion

        #region Session Correlation & SessionState
        public static Task PublishToTopicWithSessionCorrelation(string ConnectionString, string TopicName)
        {
            //generating test data -- Stock Quotes --
            List<Stock> temp = new List<Stock>();
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "AAPL", 0.01, 120.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DIS", 0.02, 105.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "VRX.TO", 0.09, 50.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "JNJ", 0.03, 100.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SAP.TO", 0.12, 35.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "DOL.TO", 0.07, 60.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "SU.TO", 0.21, 40.00, temp);
            Utils.DataGenerator.GenerateStockQuoteWithCountAndPutItInList(10, "MSFT", 0.05, 45.00, temp);

            //creating Messages out of test data
            List<Message> all_messages = temp.Select(m =>
            {

                var message = new Message(Utils.Serialize<Stock>(m))
                {
                    ContentType = "application/json",
                    MessageId = $"{m.Name}-{m.Count}-{DateTime.Now.ToString("yyyy-M-ddThh:mm:ss.ff")}",
                    Label = "Stock",
                    TimeToLive = TimeSpan.FromMinutes(10),
                    SessionId = $"{m.Name}"
                };

                //TODO: Message properties
                message.UserProperties.Add("Exchange", m.Exchange.ToString());
                message.UserProperties.Add("Region", null);
                message.UserProperties.Add("Count", m.Count);

                return message;
            }).ToList();

            //send async
            var all_tasks = new List<Task>();

            var sender = new MessageSender(ConnectionString, TopicName);

            all_messages.ForEach(m => {
                Task.Delay(new Random().Next(30)); //add a 30 ms delay to scramble the order or messages
                all_tasks.Add(sender.SendAsync(m));
            });

            return Task.WhenAll(all_tasks.ToArray());
        }

        #endregion

    }
}
