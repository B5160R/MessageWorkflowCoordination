using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory() { HostName = "localhost" };

using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
  string response_queue = "response_queue";
  string request_queue = "request_queue";

  channel.QueueDeclare(queue: request_queue, 
    durable: false, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);

  channel.QueueDeclare(queue: response_queue, 
    durable: false, 
    exclusive: false, 
    autoDelete: false, 
    arguments: null);
  
  Console.WriteLine("Acquiring work item...");

  try
  {
    // Start transaction for sending the request
    channel.TxSelect();

    var message = "Process this work item";
    var body = Encoding.UTF8.GetBytes(message);

    //Publish the request message
    channel.BasicPublish(exchange: "", 
      routingKey: request_queue, 
      basicProperties: null, 
      body: body);
    
    Console.WriteLine($" [x] Sent request: '{message}'");

    // Commit the transaction
    channel.TxCommit();

    Console.WriteLine("Request transaction committed");

    // Wait for the response
    var response = string.Empty;

    // Create a consumer for the response queue
    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (model, ea) =>
    {
      var response = Encoding.UTF8.GetString(ea.Body.ToArray());
      Console.WriteLine($" [x] Received response: '{response}'");
    };

    channel.BasicConsume(queue: response_queue, 
      autoAck: true, 
      consumer: consumer);
    
    // Wait for the response
    Console.WriteLine("Waiting for response...");
    while (string.IsNullOrEmpty(response))
    {
    }

    // Start a transaction to handle the response
    channel.TxSelect();

    // Commit the transaction after processing the response
    channel.TxCommit();

    Console.WriteLine("Response transaction committed");
  }
  catch (Exception ex)
  {
    Console.WriteLine($"An error occurred: {ex.Message}");
    // Rollback the transaction if an exception occurs
    channel.TxRollback();
    Console.WriteLine("Transaction rolled back");
  }
}