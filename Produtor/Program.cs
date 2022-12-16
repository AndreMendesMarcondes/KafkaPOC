using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using desenvolvedor.io;
using Serializer;
using System.IO.Pipes;
using System.Text;

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081",
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var config = new ProducerConfig() { BootstrapServers = "localhost:9092" };

using var producer = new ProducerBuilder<string, Curso>(config)
    .SetValueSerializer(new AvroSerializer<Curso>(schemaRegistry))
    .Build();

var result = await ProduceSerializeMessage< Curso>(new Curso { Id = Guid.NewGuid().ToString(), Descricao = "OI" });

Console.WriteLine(result.Offset);

static async Task<DeliveryResult<string, T>> ProduceSerializeMessage<T>(T message)
{
    var config = new ProducerConfig() 
    { 
        BootstrapServers = "localhost:9092",
        EnableIdempotence= true,
        Acks = Acks.All,
        MaxInFlight = 1,
        MessageSendMaxRetries = 5,

        TransactionalId = Guid.NewGuid().ToString(),
    };

    var producer = new ProducerBuilder<string, T>(config)
        .SetValueSerializer(new SerializerDevStore<T>())
        .Build();

    producer.InitTransactions(timeout: TimeSpan.FromSeconds(5));
    producer.BeginTransaction();

    var headers = new Headers();
    headers.Add("application", Encoding.UTF8.GetBytes("payment"));
    headers.Add("transactionId", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));

    var result = await producer.ProduceAsync("cursos", new Message<string, T>
    {
        Key = Guid.NewGuid().ToString(),
        Value = message,
        Headers = headers
    });

    return result;
}
