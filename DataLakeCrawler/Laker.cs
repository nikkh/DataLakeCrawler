using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.EventHubs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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


        [FunctionName("LakerTrigger")]
        public async Task<IActionResult> Trigger(
    [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
    ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            var directoryClient = fileSystemClient.GetDirectoryClient(name);
            log.LogDebug($"Processing top level directory {name}");
            
            AsyncPageable<PathItem> pathItems = directoryClient.GetPathsAsync(false);
            await foreach (var pathItem in pathItems)
            {
                if ((bool)pathItem.IsDirectory)
                {

                    Console.WriteLine($"{pathItem.Name} is a directory.  Add to processing queue.");
                    string pathItemData = JsonConvert.SerializeObject(pathItem);
                    Message message = new Message(Encoding.UTF8.GetBytes(pathItemData));
                    await _queueClient.SendAsync(message);
                }
                else
                {
                    // do nothing (files will be handled by the main function
                }
            }

            string responseMessage = $"Processing triggered for {name}";

            return new OkObjectResult(responseMessage);
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
                await messageReceiver.DeadLetterAsync(message.SystemProperties.LockToken);
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
            await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            return;
        }
    }
}
