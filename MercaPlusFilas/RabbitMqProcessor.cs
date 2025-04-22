using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MercaPlusFilas.Model;

namespace MercaPlusFilas
{
    public class RabbitMqProcessor : IDisposable
    {
        private readonly string _hostName = "localhost";
        private readonly string _mainQueue;
        private readonly string _errorQueue;
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMqProcessor(string mainQueue, string errorQueue)
        {
            _mainQueue = mainQueue;
            _errorQueue = errorQueue;

            var factory = new ConnectionFactory()
            {
                HostName = _hostName,
                Port = 5673, // Porta externa mapeada para 5672 dentro do container
                UserName = "guest",
                Password = "guest"
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(queue: _mainQueue, durable: false, exclusive: false, autoDelete: false);
            _channel.QueueDeclare(queue: _errorQueue, durable: false, exclusive: false, autoDelete: false);
        }

        public Task SendMessagesAsync(int count)
        {
            for (int i = 0; i < count; i++)
            {
                var msg = new MessageModel { Conteudo = $"Mensagem {i + 1}" };
                var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(msg));
                _channel.BasicPublish(exchange: "", routingKey: _mainQueue, basicProperties: null, body: body);
            }

            return Task.CompletedTask;
        }

        public Task ReadMessagesAsync(int? maxMessages = null)
        {
            var consumer = new EventingBasicConsumer(_channel);
            string consumerTag = _channel.BasicConsume(queue: _mainQueue, autoAck: true, consumer: consumer);

            int processed = 0;

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var obj = JsonSerializer.Deserialize<MessageModel>(message);

                Console.WriteLine($">> [{_mainQueue}] Processando: {obj?.Conteudo}");

                try
                {
                    if (obj?.Conteudo?.Contains("3") == true || obj?.Conteudo?.Contains("7") == true)
                        throw new Exception("Erro simulado!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($">> ERRO: {ex.Message}. Enviando para fila de erro.");
                    _channel.BasicPublish("", _errorQueue, null, body);
                }

                processed++;
                
            };

            _channel.BasicConsume(queue: _mainQueue, autoAck: true, consumer: consumer);

            return Task.Delay(3000); // Aguarda consumo por alguns segundos
        }

        public void Dispose()
        {
            _channel?.Close();
            _connection?.Close();
        }
    }
}
