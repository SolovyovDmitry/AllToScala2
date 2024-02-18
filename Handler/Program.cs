using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Immutable;

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
    thread.Start();

    while (true)
    {
      if (cancellationToken.IsCancellationRequested)
      {
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
      var sendResult = await _publisher.SendData(address, payload);

      if (sendResult == SendResult.Rejected)
      {
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
          break;
        }
        
        Event data = await _consumer.ReadData();
        foreach (Address address in data.Recipients)
        {
          eventsStack.Push(SendData(address, data.Payload));
        }
      }
    }
  }
}

record Payload(string Origin, byte[] Data);
record Address(string DataCenter, string NodeId);
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
