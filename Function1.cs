using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.ServiceBus;


namespace FunctionAppEvHub
{
    public static class Function1
    {
        const string ServiceBusConnectionString = "Endpoint=sb://mysvcbus1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=GezzJ8GBUP8mZz7hbdPHWHYs8da1pvO4J+nlNlfMdUY=";
        const string QueueName = "queuesb";
        static IQueueClient queueClient;

        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("eventhub", Connection = "SB")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    var message = new Message(Encoding.UTF8.GetBytes(ReverseString(messageBody)));
                    await queueClient.SendAsync(message);
                    log.LogInformation("message added to SB" + message);
                    //await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        public static string ReverseString(string s)
        {
            char[] arr = s.ToCharArray();
            Array.Reverse(arr);
            return new string(arr);
        }
    }
}
