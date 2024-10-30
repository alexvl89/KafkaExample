using Confluent.Kafka;
using System;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {

                var guid=Guid.NewGuid().ToString();

                var message = $"TransactionID: {guid} | Amount: $100 | AccountFrom: 123 | AccountTo: 456";
                var deliveryReport = await producer.ProduceAsync("transactions", new Message<Null, string> { Value = message });

                Console.WriteLine($"Message '{deliveryReport.Value}' delivered to '{deliveryReport.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Error: {ex.Error.Reason}");
            }
        }
    }
}