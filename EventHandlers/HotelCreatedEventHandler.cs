using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;
using Nest;

namespace HotelCreatedEventHandler
{
    public class HotelCreatedEventHandler
    {
        public async Task Handler(SNSEvent snsEvent)
        {
            var dbClient = new AmazonDynamoDBClient();
            var table = Table.LoadTable(dbClient, "hotel-created-event-ids");

            
            var host = Environment.GetEnvironmentVariable("host");//https://96cb61893fd34951b5018633338ab10a.europe-west3.gcp.cloud.es.io:443
            var userName = Environment.GetEnvironmentVariable("userName");//elastic               
            var password = Environment.GetEnvironmentVariable("password");//Pc5ci6F4AY0251c55ZIbqKi7
            var indexName = Environment.GetEnvironmentVariable("indexName");

            var connSettings = new ConnectionSettings(new Uri(host));
            connSettings.BasicAuthentication(userName, password);
            connSettings.DefaultIndex(indexName);
            connSettings.DefaultMappingFor<Hotel>(m => m.IdProperty(p => p.Id));

            var esClient = new ElasticClient(connSettings);

            if (!(await esClient.Indices.ExistsAsync(indexName)).Exists)
            {
                await esClient.Indices.CreateAsync(indexName);
            }

            foreach (var eventRecord in snsEvent.Records)
            {
                var eventId = eventRecord.Sns.MessageId;
                var foundItem = table.GetItemAsync(eventId);
                if (foundItem == null)
                {
                    await table.PutItemAsync(new Document
                    {
                        ["eventId"] = eventId
                    });
                }

                var hotel = JsonSerializer.Deserialize<Hotel>(eventRecord.Sns.Message);

                await esClient.IndexDocumentAsync<Hotel>(hotel);
            }
        }
    }
}