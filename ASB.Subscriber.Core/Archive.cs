using System;
using System.Collections.Generic;
using System.Text;

namespace ASB.Subscriber.Core
{
    class Archive
    {
        //static bool ArchiveMessages(string entityname, Message message, ConsoleColor color)
        //{
        //    try
        //    {
        //        var client = Utils.GetCosmosDBClient(new ConnectionPolicy()
        //        {
        //            ConnectionMode = ConnectionMode.Direct,
        //            ConnectionProtocol = Protocol.Tcp
        //        });
        //        var colluri = Utils.GetCosmosDBCollectionUri();

        //        client.CreateDocumentAsync(colluri, new ArchivedMessage { EntityName = entityname, Message = message });

        //        var s = $"\t\t\t\tMessage archived: \n\t\t\t\t\t\tMessageId = {message.MessageId}, \n\t\t\t\t\t\tSequenceNumber = {message.SystemProperties.SequenceNumber}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {message.SystemProperties.EnqueuedTimeUtc},\n\t\t\t\t\t\tEntityNameContent: {entityname}";

        //        Utils.Write(ConsoleColor.Blue, color, s);

        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error($"[{message.MessageId}] -- {ex.Message}");
        //    }

        //    return false;
        //}

        #region Archive

        //public static async Task ListenToTopicArchiveSubscriptionsAsync()
        //{
        //    var cts = new CancellationTokenSource();

        //    var allListeneres = Task.WhenAll(
        //        ReceiveMessages_FromTopicArchiveSubscriptions("poc_topic_quotedistribution", Utils.GetSubsClient("subquotedist_catchALL"), cts.Token, ConsoleColor.Red)
        //    );

        //    await Task.WhenAll(
        //        Task.WhenAny(
        //            Task.Run(() => Console.ReadKey()),
        //            Task.Delay(TimeSpan.FromMinutes(5))
        //        ).ContinueWith((t) => cts.Cancel()),
        //        allListeneres
        //        );
        //}
        //private static async Task ReceiveMessages_FromTopicArchiveSubscriptions(string EntityName, SubscriptionClient client, CancellationToken token, ConsoleColor color)
        //{
        //    var doneReceiving = new TaskCompletionSource<bool>();

        //    token.Register(
        //        async () =>
        //        {
        //            await client.CloseAsync();
        //            doneReceiving.SetResult(true);
        //        });

        //    client.RegisterMessageHandler(
        //        async (message, token1) =>
        //        {
        //            try
        //            {
        //                if (ArchiveMessages(EntityName, message, ConsoleColor.White))
        //                {
        //                    await client.CompleteAsync(message.SystemProperties.LockToken);
        //                }
        //                else
        //                {
        //                    await client.DeadLetterAsync(message.SystemProperties.LockToken, "Message could not be archived.", "Could not archive this message. Fix any errors and try again by retrieving from deadleter queue.");
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                Log.Error($"[{message.MessageId}] -- {ex.Message}");
        //                await client.AbandonAsync(message.SystemProperties.LockToken);
        //            }
        //        },
        //        GetDefaultMessageHandlingOptions());

        //    await doneReceiving.Task;
        //}



        #endregion
    }
}
