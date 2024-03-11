using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;
using Nest;
using Amazon;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace HotelCreatedEventHandler
{
    public class HotelCreatedEventHandler
    {
        public async Task Handler(SNSEvent snsEvent)
        {
            Console.WriteLine("Lambda was invoked.");
            var dbClient = new AmazonDynamoDBClient(RegionEndpoint.EUNorth1);
            var table = Table.LoadTable(dbClient, "hotel-created-event-ids");

            var host = Environment.GetEnvironmentVariable("host");
            var userName = Environment.GetEnvironmentVariable("userName");
            var password = Environment.GetEnvironmentVariable("password");
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

            Console.WriteLine($"Found {snsEvent.Records.Count} records in SNS Event");

            foreach (var eventRecord in snsEvent.Records)
            {
                var eventId = eventRecord.Sns.MessageId;
                var foundItem = await table.GetItemAsync(eventId);
                if (foundItem == null)
                {
                    await table.PutItemAsync(new Document
                    {
                        ["eventId"] = eventId
                    });

                    var hotel = JsonSerializer.Deserialize<Hotel>(eventRecord.Sns.Message);

                    // Check if the hotel already exists in ElasticSearch
                    var searchResponse = await esClient.SearchAsync<Hotel>(s => s
                        .Query(q => q
                            .Term(t => t
                                .Field(f => f.Id)
                                .Value(hotel.Id))));

                    if (!searchResponse.Hits.Any()) // If hotel doesn't exist, index it
                    {
                        var response = await esClient.IndexDocumentAsync(hotel);
                        if (response.Result == Result.Error)
                        {
                            Console.WriteLine($"Server Error: {response.ServerError.Error.Reason}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Hotel already exists with ID: {hotel.Id}");
                    }
                }
            }
        }
    }
}
