using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using desenvolvedor.io;
using Newtonsoft.Json;
using Serializer;

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
        BootstrapServers = "localhost:9092"
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
        Console.WriteLine($"{result.Message.Key} - {JsonConvert.SerializeObject(result.Message.Value)}");
    }
}