using ASB.Common.Core;
using Serilog;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ASB.Publisher.Core
{
    class Program
    {
        static void Main(string[] args)
        {
            
            //connection string to the Azure Service Bus namespace           
            string _connectionString = ConfigurationManager.ConnectionStrings["AzureServiceBusConnectionString"].ConnectionString;

            //name of the queue where the messages will be send and multiple consumers will read from 
            string _topicName = ConfigurationManager.AppSettings["QueueName"];

            //log where will log all activities and will simulate writing and consuming of messages
            //by default, will use a ColoredConsole log from https://serilog.net
            //can change this to any type of log supported by serilog sinks. also, it supported by native logging of Azure Function and Application Insights
            var _log = new LoggerConfiguration().WriteTo.ColoredConsole().CreateLogger();

            //will use this to make the multi-threading of the many consumer threads work
            var _semaphore = new SemaphoreSlim(1, 1);


            //starting creation of the queue if it doesn't exists
            //queues and topics/subscriptions of Azure Service Bus require many parameters to be set up correctly
            //avoid doing the set up manually; use and automated process instead, like a json ARM template, a powershell or azure cli script; 
            //or build some c# helper file/class as done here
            var deploy = new Deploy(_log);

            //┌─────────────────────────────┐
            //│  CREATING FILTER TOPICS     │
            //└─────────────────────────────┘

            if (!deploy.ExistsTopic(_topicName))
            {
                deploy.CreateTopic(TopicName: "topic_priority", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, MaxSize: 1024, EnableBatch: true, EnablePartitioning: false, EnableExpress: false, RequireDubsDetection: true, DuplicateDetectionTimeWindow: 1, EnforceMessageOrdering: false);

                deploy.CreateSubscription(TopicName: "topic_priority", SubscriptionName: "priority1", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_priority", SubscriptionName: "priority1", RuleName: "Main", SqlFilter: "Priority=1");

                deploy.CreateSubscription(TopicName: "topic_priority", SubscriptionName: "priority2", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_priority", SubscriptionName: "priority2", RuleName: "Main", SqlFilter: "Priority=2");

                deploy.CreateSubscription(TopicName: "topic_priority", SubscriptionName: "priority3", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_priority", SubscriptionName: "priority3", RuleName: "Main", SqlFilter: "Priority=3");


                deploy.CreateTopic(TopicName: "topic_quotedistribution", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, MaxSize: 1024, EnableBatch: true, EnablePartitioning: false, EnableExpress: false, RequireDubsDetection: true, DuplicateDetectionTimeWindow: 1, EnforceMessageOrdering: false);

                deploy.CreateSubscription(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchALL", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchALL", RuleName: "Main", SqlFilter: "1=1");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchNYSE", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchNYSE", RuleName: "Main", SqlFilter: "Exchange='NYSE'");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchNASDAQ", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchNASDAQ", RuleName: "Main", SqlFilter: "Exchange='NASDAQ'");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchTSX", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution", SubscriptionName: "subquotedist_catchTSX", RuleName: "Main", SqlFilter: "Exchange='TSX'");


                deploy.CreateTopic(TopicName: "topic_quotedistribution_with_session", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, MaxSize: 1024, EnableBatch: true, EnablePartitioning: false, EnableExpress: false, RequireDubsDetection: true, DuplicateDetectionTimeWindow: 1, EnforceMessageOrdering: false);

                deploy.CreateSubscription(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchALL", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchALL", RuleName: "Main", SqlFilter: "1=1");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchNYSE", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchNYSE", RuleName: "Main", SqlFilter: "Exchange='NYSE'");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchNASDAQ", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchNASDAQ", RuleName: "Main", SqlFilter: "Exchange='NASDAQ'");

                deploy.CreateSubscription(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchTSX", AutoDeleteOnIdleInDays: 7, DefaultTimeToLive: 10, EnableBatch: true, LockDuration: 30, MaxDeliveryCount: 10, EnableDLOnFilterEvalErrors: true, EnableDLOnMessageExpiration: false, RequiresSession: false);
                deploy.CreateFilter(TopicName: "topic_quotedistribution_with_session", SubscriptionName: "subquotedist_catchTSX", RuleName: "Main", SqlFilter: "Exchange='TSX'");

            }

            Console.Clear();
            Utils.ConsoleWriteHeader("Testing Azure Service Bus - Publisher", ConsoleColor.Red);


            var m = new ConsoleMenu()
                .With(ConsoleColor.Blue, ConsoleColor.White)

                .AddMenuItem("P", "Publish to topic", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopic, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("H", "Publish high volume to topic", ConsoleColor.Magenta, () => Wrapper(Publish.PublishHighVolumeToTopic, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("B", "Publish to topic in batch", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicInBatch, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("D", "Publish to topic in dead-letter", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithDeadlettering, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("G", "Publish with active geo-replication", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithActiveGeoreplication, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("J", "Publish with passive geo-replication", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithPassiveGeoreplication, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("Q", "Publish to topic with partitioning", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithPartitioning, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("W", "Publish to topic with priority", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithPriority, _connectionString, "topic_priority", _log))
                .AddMenuItem("E", "Publish with message scheduling", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithScheduledMessages, _connectionString, "topic_priority", _log))
                .AddMenuItem("R", "Publish with browsing and message sender", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithMessageBrowsing, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("T", "Publish to topic with message correlation", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithMessageCorrelation, _connectionString, "topic_quotedistribution", _log))
                .AddMenuItem("S", "Publish with session correlation + state", ConsoleColor.Magenta, () => Wrapper(Publish.PublishToTopicWithSessionCorrelation, _connectionString, "topic_quotedistribution_with_session", _log))
                
                .Run();


            Console.ReadKey();

        }


        static void Wrapper(Func<string, string, Task> func, string conn, string topic, ILogger log)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();

            Task.WaitAll(func(conn, topic));

            var ts = stopWatch.Elapsed;
            log.Information(" Time: {0}", String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10));
        }


    }
}
