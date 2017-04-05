package moip.beckenbauer.infra

import avro.Balance
import avro.SpecificAvroSerde
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.TopologyBuilder
import java.util.*


/**
 * Created by diegoicosta on 28/10/16.
 */
class MockedKafkaTest {

    val avroSerde = createAvroSerde()

/*
    @Test
    fun testChainedTopology() {
        MockedKafka()
                .apply(chainedTopology())
                .input(key = "key-01", value = "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = StringDeserializer())
                .matching(expectedKey = "key-01", expectedValue = "value-01")
                .toTopic(outTopic = "topic-03")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = IntegerDeserializer())
                .matching(expectedKey = "key-01", expectedValue = 1)
    }

    @Test
    fun testSimpleTopology() {
        MockedKafka()
                .apply(topologyBuilder())
                .input(key = "key-02", value = "value-02")
                .serializedBy(StringSerializer(), StringSerializer())
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = StringDeserializer())
                .matching(expectedKey = "key-01", expectedValue = "value-01")
    }

    @Test
    fun testSimpleTopologyManyInputs() {
        MockedKafka()
                .apply(topologyBuilder())
                //.input(key = "key-01", value = "value-01")
                .input(DataStream<String, String>().add("",""))
                .serializedBy(StringSerializer(), StringSerializer())
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = StringDeserializer())
                .matching(expectedKey = "key-01", expectedValue = "value-01")
    }


    @Test
    fun testSimpleAvroTopologyMocked() {

        val avroDeserializer = avroSerde.deserializer()
        val avroSerializer = avroSerde.serializer()

        MockedKafka()
                .apply(avroSimpleTopology())
                .input(key = "key-01", value = Balance(1977L))
                .serializedBy(StringSerializer(), avroSerializer)
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = avroDeserializer)
                .matching(expectedKey = "key-01", expectedValue = Balance(1977L))
    }


    @Test
    fun testAvroTopologyDirectly() {

        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mocked-${UUID.randomUUID()}")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val driver = ProcessorTopologyTestDriver(StreamsConfig(props), avroSimpleTopology())

        val value = avroSerde.serializer().serialize("topic-01", Balance(1977L))
        val key = StringSerializer().serialize("topic-01", "key-123")

        driver.process("topic-01", key, value)
    }

   */
    private fun createAvroSerde(): SpecificAvroSerde<Balance> {

        val schemaRegistry = MockSchemaRegistryClient()

        val specificDeserializerProps = HashMap<String, String>()

        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://fake-url")
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")

        val serde = SpecificAvroSerde<Balance>(schemaRegistry, specificDeserializerProps)
        return serde
    }

    private fun topologyBuilder(): TopologyBuilder {
        val builder = KStreamBuilder()
        val serde = StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .to(serde, serde, "topic-02")
        //@formatter:on

        return builder
    }

    private fun avroSimpleTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val keySerde = StringSerde()

        //@formatter:off
        builder.stream<String, Balance>(keySerde, avroSerde, "topic-01")
                .to(keySerde, avroSerde, "topic-02")
        //@formatter:on

        return builder
    }


    private fun chainedTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val serde = StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .filter({ k, v -> k != "NIE" })
                .through(serde, serde, "topic-02")
                .filter({ k, v -> k != "NOTHING" })
                .map{ key, word-> org.apache.kafka.streams.KeyValue(key, 1) }
                .to(serde, Serdes.Integer(), "topic-03")
        //@formatter:on

        return builder
    }


}