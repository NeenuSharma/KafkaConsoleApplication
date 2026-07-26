using Confluent.Kafka;
using Producer.Models;
using System.Text.Json;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer =
    new ProducerBuilder<string, string>(config)
        .Build();

var order = new OrderCreatedEvent
{
    OrderId = 105,
    ProductId = 15,
    ProductName = "Laptop",
    Quantity = 2,
    TotalAmount = 2500,
    CreatedOn = DateTime.UtcNow
};

var json = JsonSerializer.Serialize(order);

var message = new Message<string, string>
{
    Key = $"Order-{order.OrderId}",
    Value = json
};
// Send message to Kafka
var result = await producer.ProduceAsync(
    "orders",
    message);

Console.WriteLine("================================");
Console.WriteLine("Message Published Successfully!");
Console.WriteLine("================================");
Console.WriteLine($"Topic     : {result.Topic}");
Console.WriteLine($"Partition : {result.Partition}");
Console.WriteLine($"Offset    : {result.Offset}");
Console.WriteLine($"Key       : {message.Key}");
Console.WriteLine($"Value     : {message.Value}");