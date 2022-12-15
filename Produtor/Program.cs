using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using desenvolvedor.io;
using Serializer;

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
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
    var config = new ProducerConfig() { BootstrapServers = "localhost:9092" };

    var producer = new ProducerBuilder<string, T>(config)
        .SetValueSerializer(new SerializerDevStore<T>())
        .Build();

    var result = await producer.ProduceAsync("cursos", new Message<string, T>
    {
        Key = Guid.NewGuid().ToString(),
        Value = message
    });

    return result;
}
