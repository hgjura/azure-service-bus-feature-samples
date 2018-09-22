using System;
using System.Collections.Generic;
using System.Text;

namespace ASB.Publisher.Core
{
    class Archive
    {
        #region Archiving

        //public static void PullFromArchiveAndPublishToTopicWithKnownEntity(string EntityName, DateTime From, DateTime To)
        //{
        //    var messages = (from m in Utils.GetCosmosDBClient().CreateDocumentQuery<ArchivedMessage>(Utils.GetCosmosDBCollectionUri(), new FeedOptions { EnableCrossPartitionQuery = true })
        //                    where (m.EntityName == EntityName && m.Message.SystemProperties.EnqueuedTimeUtc >= From.ToUniversalTime() && m.Message.SystemProperties.EnqueuedTimeUtc <= To)
        //                    select m).ToList();
        //    messages.ForEach(m => m.Message.MessageId = $"{m.Message.MessageId}|{Guid.NewGuid()}");

        //    //send async
        //    var sender = Utils.GetMessageSender(EntityName);

        //    Stopwatch stopWatch = new Stopwatch();
        //    stopWatch.Start();

        //    messages.ForEach(m => sender.SendAsync(m.Message.Clone()));

        //    var ts = stopWatch.Elapsed;
        //    Log.Information("\nTime: {0}", String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10));

        //}

        //public static void PullFromArchiveAndPublishToTopic(DateTime From, DateTime To)
        //{

        //    var messages = (from m in Utils.GetCosmosDBClient().CreateDocumentQuery<ArchivedMessage>(Utils.GetCosmosDBCollectionUri(), new FeedOptions { EnableCrossPartitionQuery = true })
        //                    where (m.Message.SystemProperties.EnqueuedTimeUtc >= From.ToUniversalTime() && m.Message.SystemProperties.EnqueuedTimeUtc <= To)
        //                    select m).ToList();
        //    messages.ForEach(m => m.Message.MessageId = $"{m.Message.MessageId}|{Guid.NewGuid()}");

        //    //send async

        //    Stopwatch stopWatch = new Stopwatch();
        //    stopWatch.Start();

        //    messages.ForEach(m => {
        //        var sender = Utils.GetMessageSender(m.EntityName);
        //        sender.SendAsync(m.Message.Clone());
        //    });

        //    var ts = stopWatch.Elapsed;
        //    Log.Information("\nTime: {0}", String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10));

        //}

        #endregion
    }
}
