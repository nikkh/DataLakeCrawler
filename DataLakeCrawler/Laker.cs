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
        string CreateQueue;
        string TerminateCreate;

        string sasToken;
        Uri serviceUri;
        static DataLakeServiceClient serviceClient;
        static DataLakeFileSystemClient fileSystemClient;
        static QueueClient _queueClient;
        string fileSystemName;
        private readonly TelemetryClient telemetryClient;
        public Laker(IConfiguration config, TelemetryConfiguration configuration)
        {
            telemetryClient = new TelemetryClient(configuration);
            ServiceBusConnection = config["ServiceBusConnection"];
            ServiceBusQueue = config["ServiceBusQueue"];
            CreateQueue = config["CreateQueue"];
            TerminateCreate = config["TerminateCreate"];
            sasToken = config["sasToken"];
            serviceUri = new Uri(config["serviceUri"]);
            fileSystemName = config["fileSystemName"];
            if (serviceClient == null) 
            {
                serviceClient = new DataLakeServiceClient(serviceUri, new AzureSasCredential(sasToken));
            }

            if (fileSystemClient == null)
            {
                fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);
            }
            
            if(_queueClient == null)
            {
                var csb = new ServiceBusConnectionStringBuilder(ServiceBusConnection);
                csb.EntityPath = ServiceBusQueue;
                _queueClient = new QueueClient(csb);
            }
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
    [return: EventHub("lakehub", Connection = "EventHubConnection")]
        public async Task<string> Run([ServiceBusTrigger("%ServiceBusQueue%", Connection = "ServiceBusConnection")] Message message, ILogger log, MessageReceiver messageReceiver)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            telemetryClient.GetMetric("LakeMessagesReceived").TrackValue(1);
            
            // create output object
            CrawlerResult cr = new CrawlerResult();
            
            // parse incoming message
            string payload = System.Text.Encoding.UTF8.GetString(message.Body);
            var jPayload = JObject.Parse(payload);
            var name = jPayload["Name"].ToString();
            cr.Path = name;
            cr.IsDirectory = Boolean.Parse(jPayload["IsDirectory"].ToString());

            log.LogInformation($"ProcessLakeFolder: Message {message.MessageId} dequeued.  Path is {name}");

            // If this is not a directory then error
            if (cr.IsDirectory)
            {
                telemetryClient.TrackEvent($"Directory processing request", 
                        new Dictionary<string, string>() { { "path", cr.Path } });
            }
            else
            {
                Exception e = new Exception("Laker invoked by passing a file. This is not supported!");
                telemetryClient.TrackException(e);
                await messageReceiver.DeadLetterAsync(message.SystemProperties.LockToken);
                throw e;
            }

            // Get a client to read metadata for this directory and increment metrics for success or failure            
            DataLakeDirectoryClient directoryClient = null;
            try
            {
                directoryClient = fileSystemClient.GetDirectoryClient(name);
                telemetryClient.GetMetric("LakeDirClientSuccess").TrackValue(1);
            }
            catch (Exception e)
            {
                telemetryClient.TrackException(e);
                telemetryClient.GetMetric("LakeDirClientFailure").TrackValue(1);
                telemetryClient.TrackTrace($"Salamander - Attempt directory client for  {name} failed.  Exception was {e}");
            }

            // Get access control for this directory and increment metrics for success or failure
            Response<PathAccessControl> aclResult = null;
            try
            {
                aclResult = await directoryClient.GetAccessControlAsync();
                telemetryClient.GetMetric("LakeDirAclSuccess").TrackValue(1);
            }
            catch (Exception e)
            {
                telemetryClient.TrackException(e);
                telemetryClient.GetMetric("LakeDirAclFailue").TrackValue(1);
                telemetryClient.TrackTrace($"Salamander - Attempt to retrieve acl for directory {name} failed.  Exception was {e}");
                throw e;
            }

            // add acls to output object
            foreach (var item in aclResult.Value.AccessControlList)
            {
                cr.ACLs.Add(item);
            }

            // read contents of this directory
            AsyncPageable<PathItem> pathItems=null;
            try
            {
               pathItems = directoryClient.GetPathsAsync(false);
               telemetryClient.GetMetric("LakeDirPathSuccess").TrackValue(1);
            }
            catch (Exception e)
            {
                telemetryClient.TrackException(e);
                telemetryClient.GetMetric("LakeDirPathFailure").TrackValue(1);
                telemetryClient.TrackTrace($"Salamander - Attempt to process directory {name} failed.  Exception was {e}");
                throw e;
            }

            // For each item in the directory
            await foreach (var pathItem in pathItems)
            {
                // if it's a directory, just send a message to get it processed.
                if ((bool)pathItem.IsDirectory)
                {
                    log.LogWarning($"{pathItem.Name} is a directory.  Add to processing queue.");
                    string data = JsonConvert.SerializeObject(pathItem);
                    Message newMessage = new Message(Encoding.UTF8.GetBytes(data));
                    telemetryClient.GetMetric("LakeDirMessagesGenerated").TrackValue(1);
                    await _queueClient.SendAsync(newMessage);
                }
                // if it's a file, get its acls
                else
                {
                   log.LogDebug($"File {pathItem} will be added to output");
                   var fileClient = fileSystemClient.GetFileClient(pathItem.Name);
                   CrawlerFile cf = new CrawlerFile();
                   cf.Name = pathItem.Name;
                    // Get access control for this file and increment metrics for success or failure
                    try
                    {
                       aclResult = await fileClient.GetAccessControlAsync();
                       telemetryClient.GetMetric("LakeFileAclSuccess").TrackValue(1);
                   }
                   catch (Exception e)
                   {
                       telemetryClient.TrackException(e);
                       telemetryClient.GetMetric("LakeFileAclFailure").TrackValue(1);
                       telemetryClient.TrackTrace($"Salamander - Attempt to retrieve acl for file {pathItem} failed.  Exception was {e}");
                       throw e;
                    }

                    // add acls to file output object       
                    foreach (var item in aclResult.Value.AccessControlList)
                    {
                        cf.ACLs.Add(item);
                    }
                    cr.Files.Add(cf);
                }
            }


            log.LogInformation($"ProcessLakeFolder: Message {message.MessageId} processed.  Path was {name}");
            telemetryClient.GetMetric("LakeMessagesProcessingDurationMs").TrackValue(watch.ElapsedMilliseconds);
            telemetryClient.GetMetric("LakeMessagesProcessed").TrackValue(1);
            return JsonConvert.SerializeObject(cr);
        }

        [FunctionName("CreateLakeFolder")]
        public async void RunCreate([ServiceBusTrigger("%CreateQueue%", Connection = "ServiceBusConnection")] Message message, ILogger log, MessageReceiver messageReceiver)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            telemetryClient.GetMetric("CreateLakeMessagesReceived").TrackValue(1);

            // create output object
            CrawlerResult cr = new CrawlerResult();

            // parse incoming CreateRequest message
            string payload = System.Text.Encoding.UTF8.GetString(message.Body);
            CreateRequest request = JsonConvert.DeserializeObject<CreateRequest>(payload);

            // if we are at the max path, do nothing - your work here is complete
            if(request.CurrentDepth >= request.MaxDepth || TerminateCreate == "1")
            {
                log.LogInformation($"CreateLakeFolder: Message {message.MessageId} dequeued.  No more to do");
                return;
            }

            // increment the path, or it will never stop
            request.CurrentDepth++;

            log.LogInformation($"CreateLakeFolder: Message {message.MessageId} dequeued.  Path is {request.Path}");

            // using path, set where we are in terms of the current directory
            var directoryClient = fileSystemClient.GetDirectoryClient(request.Path);

            // create a number of files
            for (int i = 0; i < request.NumberOfFiles; i++)
            {

                // TODO set ACLs for the file
            }

            // create a number of directories and fire off a service bus message for each
            for (int i = 0; i < request.NumberOfFDirectories; i++)
            {
                // create directory
                var newDir = string.Format("{1}{2}", request.DirectoryPattern, i);
                await directoryClient.CreateSubDirectoryAsync(newDir);

                // TODO set ACL for the directory

                // send a service bus message with this new path
                var childRequest = request;
                childRequest.Path = request.Path + "/" + newDir; // new path
                string data = JsonConvert.SerializeObject(childRequest);
                Message newMessage = new Message(Encoding.UTF8.GetBytes(data));
                await _queueClient.SendAsync(newMessage);

            }

            // tracking
            telemetryClient.TrackEvent($"Directories created",
                    new Dictionary<string, string>{ {"path", request.Path }, {"number", request.NumberOfFDirectories.ToString() } } );
        }
    }
}
