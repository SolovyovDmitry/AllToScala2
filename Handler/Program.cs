using Microsoft.Extensions.Logging;

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
    //TODO: place code here
     
    return Task.CompletedTask;
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
