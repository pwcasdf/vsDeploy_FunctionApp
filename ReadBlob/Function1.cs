using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Devices;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ReadBlob
{
    public static class Function1
    {
        [FunctionName("ReadBlobFromFunctionApp")]
        public static async void Run([ServiceBusTrigger("c2dreadblobqueue", Connection = "connectionString")]string myQueueItem, ILogger log)
        {
            string connectionStringIoTHub = "IoTHub_Connection_String";
            string connectionStringSA = "Storage_Account_Connection_String";

            var jsonInput = (JObject)JsonConvert.DeserializeObject(myQueueItem);
            var commandType = ((JValue)jsonInput["CommandType"]).ToString();
            var deviceName = ((JValue)jsonInput["DeviceName"]).ToString();

            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionStringSA);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(deviceName);

                dynamic jsonOutput = new JObject();
                jsonOutput.Add("CommandType", commandType);
                jsonOutput.Add("DeviceName", deviceName);
                string jsonOutputString = jsonOutput.ToString(Formatting.None);

                ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(connectionStringIoTHub);
                ReadBlobHierarchical(container, DateTime.Now.ToString("yyyy/MM/dd"), serviceClient, deviceName);
            }
            catch(Exception e)
            {
                throw;
            }
        }


        private static async Task ReadBlobHierarchical(CloudBlobContainer container, string prefix, ServiceClient serviceClient, string deviceName)
        {
            CloudBlobDirectory dir;
            CloudBlob cloudBlob;
            CloudBlockBlob cloudBlockBlob;
            BlobContinuationToken continuationToken = null;

            string contents;

            try
            {
                // Call the listing operation and enumerate the result segment.
                // When the continuation token is null, the last segment has been returned and
                // execution can exit the loop.
                do
                {
                    BlobResultSegment resultSegment = await container.ListBlobsSegmentedAsync(prefix,
                        false, BlobListingDetails.Metadata, null, continuationToken, null, null);
                    foreach (var blobItem in resultSegment.Results)
                    {
                        // A hierarchical listing may return both virtual directories and blobs.
                        if (blobItem is CloudBlobDirectory)
                        {
                            dir = (CloudBlobDirectory)blobItem;

                            // Call recursively with the prefix to traverse the virtual directory.
                            await ReadBlobHierarchical(container, dir.Prefix, serviceClient, deviceName);
                        }
                        else
                        {
                            // Write out the name of the blob.
                            cloudBlob = (CloudBlob)blobItem;
                            cloudBlockBlob = container.GetBlockBlobReference(cloudBlob.Name);
                            contents = cloudBlockBlob.DownloadTextAsync().Result;

                            var serviceMessage = new Microsoft.Azure.Devices.Message(Encoding.ASCII.GetBytes(contents));
                            await serviceClient.SendAsync(deviceName, serviceMessage);
                        }
                    }

                    // Get the continuation token and loop until it is null.
                    continuationToken = resultSegment.ContinuationToken;

                } while (continuationToken != null);
            }
            catch (StorageException e)
            {
                throw;
            }
        }
    }
}
