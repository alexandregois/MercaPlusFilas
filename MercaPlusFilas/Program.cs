// Program.cs  
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using MercaPlusFilas;

var config = new ConfigurationBuilder()
   .AddJsonFile("appsettings.json")
   .Build();

string connectionString = config["ServiceBus:ConnectionString"]!;
string queueName = "teste-servicebus";
string errorQueue = "log-erros";

Console.WriteLine("[Azure Service Bus]");
await using (var processor = new MercaPlusFilas.ServiceBusProcessor(connectionString, queueName, errorQueue))
{
    Console.WriteLine("[1] Enviando 10 mensagens para a fila...");
    await processor.SendMessagesAsync(10);

    Console.WriteLine("[2] Lendo e processando mensagens (com simulação de erro)...");
    await processor.ReadMessagesAsync(queueName, maxMessages: 5);

    Console.WriteLine("[3] Aguarde alguns segundos antes de processar o restante...");
    await Task.Delay(TimeSpan.FromSeconds(10));

    Console.WriteLine("[4] Lendo mensagens pendentes + fila de erro...");
    await processor.ReadMessagesAsync(queueName);
    await processor.ReadMessagesAsync(errorQueue);

    Console.WriteLine("[5] Espiando mensagens que ainda estão na fila principal:");
    await processor.PeekMessagesAsync(queueName);

    Console.WriteLine("[6] Espiando mensagens na fila de erro:");
    await processor.PeekMessagesAsync(errorQueue);
}

Console.WriteLine("\n[RabbitMQ]");
using (var rabbit = new RabbitMqProcessor("teste-rabbitmq", "log-erros"))
{
    Console.WriteLine("[1] Enviando 10 mensagens para a fila...");
    await rabbit.SendMessagesAsync(10);

    Console.WriteLine("[2] Lendo e processando mensagens (com simulação de erro)...");
    await rabbit.ReadMessagesAsync(maxMessages: 5);

    Console.WriteLine("[3] Aguarde alguns segundos antes de processar o restante...");
    await Task.Delay(TimeSpan.FromSeconds(10));

    Console.WriteLine("[4] Lendo mensagens pendentes + fila de erro...");
    await rabbit.ReadMessagesAsync();
}