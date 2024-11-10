using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory() { HostName = "localhost" };

using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
  // Declare the request and response queues
  string requestQueueName = "request_queue";
  string responseQueueName = "response_queue";

  channel.QueueDeclare(queue: requestQueueName, 
    durable: false, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);

  channel.QueueDeclare(queue: responseQueueName, 
    durable: false, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);

  var consumer = new EventingBasicConsumer(channel);
  consumer.Received += (model, ea) =>
  {
    var requestMessage = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($" [x] Received request: '{requestMessage}'");

    // Process the request (simulated work)
    string replyMessage = $"Processed: {requestMessage}";

    try
    {
      // Simulate processing and then reply
      // Start transaction for sending the reply
      channel.TxSelect();
      var replyBody = Encoding.UTF8.GetBytes(replyMessage);
      channel.BasicPublish(exchange: "", 
        routingKey: responseQueueName, 
        basicProperties: null, 
        body: replyBody);
      Console.WriteLine($" [x] Sent reply: '{replyMessage}'");

      // Commit the transaction
      channel.TxCommit();
      Console.WriteLine("Reply transaction committed.");
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error sending reply: {ex.Message}");
      // Optionally rollback the transaction in case of errors
      channel.TxRollback();
      Console.WriteLine("Reply transaction rolled back.");
    }
  };

  channel.BasicConsume(queue: requestQueueName, 
    autoAck: false, 
    consumer: consumer);
  Console.WriteLine("Waiting for requests...");
  Console.ReadLine();
}