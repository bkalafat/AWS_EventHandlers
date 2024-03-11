using System.Text.Json;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;

//TEST URL PASS
Environment.SetEnvironmentVariable("host", "https://96cb61893fd34951b5018633338ab10a.europe-west3.gcp.cloud.es.io:443");
Environment.SetEnvironmentVariable("userName", "elastic");
Environment.SetEnvironmentVariable("password", "Pc5ci6F4AY0251c55ZIbqKi7");
Environment.SetEnvironmentVariable("indexName", "event");

var hotel = new Hotel
{
    Name = "Continental",
    City = "Paris",
    Price = 100,
    Rating = 4,
    Id = "123",
    UserId = "ABC",
    CreationDateTime = DateTime.Now
};

var snsEvent = new SNSEvent
{
    Records = new List<SNSEvent.SNSRecord>
    {
        new SNSEvent.SNSRecord()
        {
            Sns = new SNSEvent.SNSMessage
            {
                MessageId = "100",
                Message = JsonSerializer.Serialize(hotel)
            }
        }
    }
};

var handler = new HotelCreatedEventHandler.HotelCreatedEventHandler();
await handler.Handler(snsEvent);