using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Devices;

namespace DataPass
{
    public static class Function1
    {
        [FunctionName("DataPass")]
        public static async void Run([ServiceBusTrigger("d2dqueue", Connection = "connectionString")]string myQueueItem, ILogger log)
        {
            string connectionStringIoTHub = "IoTHub_Connection_String";

            var jsonInput = (JObject)JsonConvert.DeserializeObject(myQueueItem);
            var deviceName = ((JValue)jsonInput["DeviceName"]).ToString();
            var destination = ((JValue)jsonInput["To"]).ToString();
            var temperature = ((JValue)jsonInput["temperature"]).ToString();
            dynamic jsonOutput = new JObject();
            jsonOutput.Add("From", deviceName);
            jsonOutput.Add("To", destination);
            jsonOutput.Add("temperature", temperature);
            string jsonOutputString = jsonOutput.ToString(Formatting.None);

            ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(connectionStringIoTHub);
            var serviceMessage = new Microsoft.Azure.Devices.Message(Encoding.ASCII.GetBytes(jsonOutputString));
            await serviceClient.SendAsync(destination, serviceMessage);
        }
    }
}
