using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using MercaPlusFilas.Model;
using System.Text.Json;

namespace MercaPlusFilas
{
    public class ServiceBusProcessor
    {
        private readonly string _connectionString;
        private readonly string _mainQueue;
        private readonly string _errorQueue;
        private readonly ServiceBusClient _client;
        private readonly ServiceBusSender _errorSender;

        public ServiceBusProcessor(string connectionString, string mainQueue, string errorQueue)
        {
            _connectionString = connectionString;
            _mainQueue = mainQueue;
            _errorQueue = errorQueue;
            _client = new ServiceBusClient(_connectionString);
            _errorSender = _client.CreateSender(_errorQueue);
        }

        public async Task SendMessagesAsync(int count)
        {
            var sender = _client.CreateSender(_mainQueue);

            for (int i = 0; i < count; i++)
            {
                var msg = new MessageModel { Conteudo = $"Mensagem {i + 1}" };
                var body = JsonSerializer.Serialize(msg);
                await sender.SendMessageAsync(new ServiceBusMessage(body));
            }

            await sender.DisposeAsync();
        }

        public async Task ReadMessagesAsync(string queueName, int? maxMessages = null)
        {
            var receiver = _client.CreateReceiver(queueName);
            int processed = 0;

            while (true)
            {
                var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
                if (message == null || (maxMessages.HasValue && processed >= maxMessages))
                    break;

                try
                {
                    var body = message.Body.ToString();
                    var obj = JsonSerializer.Deserialize<MessageModel>(body);
                    Console.WriteLine($">> [{queueName}] Processando: {obj?.Conteudo}");

                    if (obj?.Conteudo?.Contains("3") == true || obj?.Conteudo?.Contains("7") == true)
                        throw new Exception("Erro simulado!");

                    await receiver.CompleteMessageAsync(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($">> ERRO: {ex.Message}. Enviando para fila de erro.");
                    await _errorSender.SendMessageAsync(new ServiceBusMessage(message.Body));
                    await receiver.CompleteMessageAsync(message);
                }

                processed++;
            }

            await receiver.DisposeAsync();
        }

        public async Task PeekMessagesAsync(string queueName, int maxMessages = 10)
        {
            var receiver = _client.CreateReceiver(queueName);

            var messages = await receiver.PeekMessagesAsync(maxMessages);

            if (messages.Count == 0)
            {
                Console.WriteLine($">> [PEEK] Nenhuma mensagem na fila: {queueName}");
            }

            foreach (var msg in messages)
            {
                Console.WriteLine($">> [PEEK:{queueName}] {msg.Body}");
            }

            await receiver.DisposeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await _errorSender.DisposeAsync();
            await _client.DisposeAsync();
        }
    }
}
