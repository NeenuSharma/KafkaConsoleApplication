using Confluent.Kafka;
using Consumer.Models;
using System.Text.Json;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer =
    new ConsumerBuilder<string, string>(config)
        .Build();

consumer.Subscribe("orders");

while (true)
{
    var result = consumer.Consume();
    var order = JsonSerializer.Deserialize<OrderCreatedEvent>(
        result.Message.Value);

    Console.WriteLine("-----------------------------------");
    Console.WriteLine($"Order Id      : {order!.OrderId}");
    Console.WriteLine($"Product Id    : {order.ProductId}");
    Console.WriteLine($"Product Name  : {order.ProductName}");
    Console.WriteLine($"Quantity      : {order.Quantity}");
    Console.WriteLine($"Total Amount  : {order.TotalAmount}");
    Console.WriteLine($"Created On    : {order.CreatedOn}");
    Console.WriteLine("-----------------------------------");
}
