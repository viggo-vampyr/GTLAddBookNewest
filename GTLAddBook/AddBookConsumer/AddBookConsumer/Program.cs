using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

var factory = new ConnectionFactory { HostName = "rabbitmq" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: "translated-book_queue", durable: true, exclusive: false,
    autoDelete: false, arguments: null);

await channel.ExchangeDeclareAsync(exchange: "broadcast_database", type: ExchangeType.Fanout);

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");
    
    string broadcastMessage = $"{message} has been saved in database";

    var transformedMessageJson = JsonSerializer.Serialize(broadcastMessage);

    var outputBody = Encoding.UTF8.GetBytes(transformedMessageJson);

    await channel.BasicPublishAsync(exchange: "broadcast_database", routingKey: string.Empty,
         body: outputBody);


    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
};


await channel.BasicConsumeAsync("translated-book_queue", autoAck: false, consumer: consumer);

string isDocker = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");

if (isDocker == null)
{
    Console.WriteLine("Press [enter] to exit");
    Console.ReadLine();
}
else
{
    Console.WriteLine("Hotel California"); //Gør det til en bagground service 
    Thread.Sleep(Timeout.Infinite);
}