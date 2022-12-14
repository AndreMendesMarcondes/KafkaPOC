using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using desenvolvedor.io;
using Newtonsoft.Json;
using Serializer;
using System.Text;

await ConsumeCustomDeserializer<Curso>();

static async Task ConsumeCustomDeserializeAvro()
{
    var schemaConfig = new SchemaRegistryConfig
    {
        Url = "http://localhost:8081"
    };

    var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

    var config = new ConsumerConfig()
    {
        GroupId = "devio",
        BootstrapServers = "localhost:9092",
        EnableAutoCommit= false,
        EnableAutoOffsetStore= false,

        IsolationLevel = IsolationLevel.ReadCommitted,
    };

    using var consumer = new ConsumerBuilder<string, Curso>(config)
        .SetValueDeserializer(new AvroDeserializer<Curso>(schemaRegistry).AsSyncOverAsync())
        .Build();

    consumer.Subscribe("cursos");

    while (true)
    {
        var result = consumer.Consume();
        Console.WriteLine($"{result.Message.Key} - {JsonConvert.SerializeObject(result.Message.Value)}");
    }
}

static async Task ConsumeCustomDeserializer<T>()
{
    var schemaConfig = new SchemaRegistryConfig
    {
        Url = "http://localhost:8081"
    };

    var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

    var config = new ConsumerConfig()
    {
        Acks = Acks.None, //verificando o ack
        GroupId = "devio",
        BootstrapServers = "localhost:9092"
    };

    using var consumer = new ConsumerBuilder<string, T>(config)
        .SetValueDeserializer(new DeserializerDevStore<T>())
        .Build();

    consumer.Subscribe("cursos");

    while (true)
    {
        var result = consumer.Consume();
        var message = result.Message.Value;

             var headers = result.Message.Headers.ToDictionary(p => p.Key, p => Encoding.UTF8.GetString(p.GetValueBytes()));

        var application = headers["application"];
        var transactionId = headers["transactionId"];

        Console.WriteLine("application: " + application + " - transactionId: " + transactionId);

        Console.WriteLine($"{result.Message.Key} - {JsonConvert.SerializeObject(result.Message.Value)}");
    }
}