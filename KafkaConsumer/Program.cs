using Confluent.Kafka;
using System;
using System.Threading;

class Program
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "transaction-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("transactions");

            var cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cancellationTokenSource.Token);
                    Console.WriteLine($"Received message: {consumeResult.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}