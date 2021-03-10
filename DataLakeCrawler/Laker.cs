using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.EventHubs;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace DataLakeCrawler
{

    

    public class Laker
    {

        string ServiceBusConnection;
        string ServiceBusQueue;
        private QueueClient _queueClient;
        string sasToken;
        Uri serviceUri;
        DataLakeServiceClient serviceClient;
        DataLakeFileSystemClient fileSystemClient;
        string fileSystemName;
        private readonly TelemetryClient telemetryClient;
        public Laker(IConfiguration config, TelemetryConfiguration configuration)
        {
            telemetryClient = new TelemetryClient(configuration);
            ServiceBusConnection = config["ServiceBusConnection"];
            ServiceBusQueue = config["ServiceBusQueue"];
            sasToken = config["sasToken"];
            serviceUri = new Uri(config["serviceUri"]);
            fileSystemName = config["fileSystemName"];
            serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(sasToken));
            fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
            var csb = new ServiceBusConnectionStringBuilder(ServiceBusConnection);
            csb.EntityPath = ServiceBusQueue;
            _queueClient = new QueueClient(csb);
        }

        [FunctionName("ProcessLakeFolder")]
       // [return: EventHub("lakehub", Connection = "EventHubConnection")]
        public async Task Run([ServiceBusTrigger("lake-queue", Connection = "ServiceBusConnection")] Message message, ILogger log, MessageReceiver messageReceiver)
        {
            CrawlerResult cr = new CrawlerResult();
            Stopwatch w = new Stopwatch();
            string payload = System.Text.Encoding.UTF8.GetString(message.Body);
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {payload}");
            var x = JObject.Parse(payload);
            var name = x["Name"].ToString();
            cr.Path = name;
            cr.IsDirectory = Boolean.Parse(x["IsDirectory"].ToString());
            if (cr.IsDirectory)
            {
                telemetryClient.TrackEvent($"Directory processing request recieved for directory {cr.Path} was recieved");
            }
            else
            {
                Exception e = new Exception("Laker invoked by passing a file. This is not supported!");
                telemetryClient.TrackException(e);
                throw e;
            }
            w.Start();
            var directoryClient = fileSystemClient.GetDirectoryClient(name);
            log.LogDebug($"Time to obtain directory client was {w.ElapsedMilliseconds} ms");
            
            w.Reset();
            var aclResult = await directoryClient.GetAccessControlAsync();
            log.LogDebug($"Time to GetAccessControlAsync was {w.ElapsedMilliseconds} ms");
            foreach (var item in aclResult.Value.AccessControlList)
            {
                cr.ACLs.Add(item);
            }
            log.LogDebug($"{cr.ACLs.Count} ACLS are present");
            log.LogInformation($"Processing directory {name}");
           
            AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
            await foreach (var pathItem in pathItems)
            {

                if ((bool)pathItem.IsDirectory)
                {
                    log.LogWarning($"{pathItem.Name} is a directory.  Add to processing queue.");
                    string data = JsonConvert.SerializeObject(pathItem);
                    Message newMessage = new Message(Encoding.UTF8.GetBytes(data));
                    await _queueClient.SendAsync(newMessage);
                }
                else
                {
                   log.LogDebug($"File {pathItem} will be added to output");
                   var fileClient = fileSystemClient.GetFileClient(pathItem.Name);
                    CrawlerFile cf = new CrawlerFile();
                    cf.Name = pathItem.Name;
                    w.Reset();
                    aclResult = await fileClient.GetAccessControlAsync();
                    log.LogDebug($"Time to GetAccessControlAsync was {w.ElapsedMilliseconds} ms");
                   
                    foreach (var item in aclResult.Value.AccessControlList)
                    {
                        cf.ACLs.Add(item);
                    }
                    cr.Files.Add(cf);
                }
            }
            return;
        }
    }
}
