Описание задачи, C#

Имеется канал. в который публикуются непрерывный поток данных. Есть набор потребителей этих данных. 

Необходимо реализовать метод, который будет получать данные из канала и публиковать их всем потребителям.




Технические детали

1. Чтение данных из канала осуществляется с помощью метода `IConsumer.ReadData`.

2. Публикация данных осуществляется с помощью метода `IPublisher.SendData`. Данные для отправки совпадают с данными, полученными из канала.

3. Список клиентов, которые должны получить сообщение указан в самом сообщении (поле `Recipients`).

4. Отправка может завершиться с один из следующих статусов: 

  * `SendResult.Accepted` - принято потребителем, операция отправки данных адресату считается завершённой

  * `SendResult.Rejected` - отклонено, операцию отправки следует повторить после задержки `IHandler.Timeout`

5. Метод `PerformOperation` должен обладать высокой пропускной способностью: события внутри `ReadData` **могут накапливаться**.




В качестве ответа пришлите полную реализацию интерфейса `IHandler` (со списком using'ов). В поле для ответа прикрепите ссылку на репозиторий с вашим решением.




Сниппеты кода

```csharp
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
    Ilogger<Handler> logger)
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
```
