using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace Handler;

class Program
{
  static void Main(string[] args)
  {
    Console.WriteLine("Hello, World!");
  }
}

interface IHandler 
{
  TimeSpan Timeout { get; }
   
  Task PerformOperation(CancellationToken cancellationToken);
}

class Handler: IHandler
{
  private readonly IConsumer _consumer;
  private readonly IPublisher _publisher;
  private readonly ILogger<Handler> _logger;
   
  public TimeSpan Timeout { get; }
   
  public Handler(
    TimeSpan timeout,
    IConsumer consumer,
    IPublisher publisher,
    ILogger<Handler> logger)
  {
    Timeout = timeout;
     
    _consumer = consumer;
    _publisher = publisher;   
    _logger = logger;
  }
   
  public Task PerformOperation(CancellationToken cancellationToken)
  {
    ConcurrentStack<Task> eventsStack = new();
    var thread = new Thread(() => RetrieveData(cancellationToken).Start());
    
    _logger.LogInformation("Starting to retrieve data from consumer");
    
    thread.Start();

    while (true)
    {
      if (cancellationToken.IsCancellationRequested)
      {
        _logger.LogWarning("Cancellation requested - stopping to handle publishing");
        
        break;
      }
      
      if (eventsStack.TryPop(out var sendingDataTask))
      {
        var sendDataThread = new Thread(() => sendingDataTask.Start());
        sendDataThread.Start();
      }
    }

    return Task.CompletedTask;

    async Task SendData(Address address, Payload payload)
    {
      _logger.LogInformation("Sending data to {address}", address);
      
      var sendResult = await _publisher.SendData(address, payload);

      if (sendResult == SendResult.Rejected)
      {
        _logger.LogInformation(
          "Publishing message to {address} was rejected. Retrying in {timeout}",
          address,
          Timeout);
        
        await Task.Delay(Timeout, cancellationToken);
        eventsStack.Push(SendData(address, payload));
      }
    }

    async Task RetrieveData(CancellationToken ct)
    {
      while (true)
      {
        if (ct.IsCancellationRequested)
        {
          _logger.LogWarning("Cancellation requested. Stopping to retrieve data");
          
          break;
        }
        
        Event data = await _consumer.ReadData();
        foreach (Address address in data.Recipients)
        {
          _logger.LogInformation("Retrieved data from {address}. Starting to publish", address);
          
          eventsStack.Push(SendData(address, data.Payload));
        }
      }
    }
  }
}

record Payload(string Origin, byte[] Data);
record Address(string DataCenter, string NodeId)
{
  public override string ToString()
  {
    return $"Data center {DataCenter} with Node {NodeId}";
  }
}
record Event(IReadOnlyCollection<Address> Recipients, Payload Payload);

enum SendResult
{
  Accepted,
  Rejected
}

interface IConsumer
{
  Task<Event> ReadData();
}

interface IPublisher
{
  Task<SendResult> SendData(Address address, Payload payload);   
}
