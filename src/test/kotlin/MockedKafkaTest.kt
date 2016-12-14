package moip.beckenbauer.infra

import avro.Balance
import avro.SpecificAvroDeserializer
import avro.SpecificAvroSerde
import avro.SpecificAvroSerializer
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.TopologyBuilder
import org.junit.Test
import java.util.*


/**
 * Created by diegoicosta on 28/10/16.
 */
class MockedKafkaTest {

    private fun topologyBuilder(): TopologyBuilder {
        val builder = KStreamBuilder()
        val serde = Serdes.StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .to(serde, serde, "topic-02")
        //@formatter:on

        return builder
    }

    private fun avroSimpleTopology(): TopologyBuilder {
        val schemaRegistry = MockSchemaRegistryClient()
        schemaRegistry.register("null",Balance.`SCHEMA$`)


        val specificDeserializerProps = HashMap<String, String>()
        // Intentionally invalid schema registry URL to satisfy the config class's requirement that
        // it be set.
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus")
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")


        val builder = KStreamBuilder()
        val keySerde = Serdes.StringSerde()
        val avroSerde = SpecificAvroSerde<Balance>(schemaRegistry, specificDeserializerProps)

        //@formatter:off
        builder.stream<String, Balance>(keySerde, avroSerde, "topic-01")
                .to(keySerde, avroSerde, "topic-02")
        //@formatter:on

        return builder
    }

    private fun chainedTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val serde = Serdes.StringSerde()

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


    @Test
    fun testChainedTopologyUsingMock() {
        MockedKafka()
                .apply(chainedTopology())
                .streaming(key = "key-01", value = "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = StringDeserializer())
                .matching(expectedKey = "key-01", expectedValue = "value-01")
                .toTopic(outTopic = "topic-03")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = IntegerDeserializer())
                .matching(expectedKey = "key-01", expectedValue = 1)
    }

    @Test
    fun testSimpleTopologyUsingMock() {
        MockedKafka()
                .apply(topologyBuilder())
                .streaming(key = "key-01", value = "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = StringDeserializer())
                .matching(expectedKey = "key-01", expectedValue = "value-01")
    }

    @Test
    fun testSimpleAvroTopologyUsingMock() {

        val schemaRegistry = MockSchemaRegistryClient()
        val schemaRegistry2 = MockSchemaRegistryClient()
        //schemaRegistry.register("null",Balance.`SCHEMA$`)
        //schemaRegistry.register("null",Balance.`SCHEMA$`)

        val specificDeserializerProps = HashMap<String, String>()
        // Intentionally invalid schema registry URL to satisfy the config class's requirement that
        // it be set.
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus")
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")

        val avroSerializer = SpecificAvroSerializer<Balance>(schemaRegistry)
        val avroDeserializer = SpecificAvroDeserializer<Balance>(schemaRegistry, specificDeserializerProps)
        val avroDeserializer2 = SpecificAvroDeserializer<Balance>(schemaRegistry2, specificDeserializerProps)


        MockedKafka()
                .apply(avroSimpleTopology())
                .streaming(key = "key-01", value = Balance(1977L))
                .serializedBy(StringSerializer(), avroSerializer)
                .fromTopic(name = "topic-01").toTopic(outTopic = "topic-02")
                .deserializedBy(keyDeserializer = StringDeserializer(), valueDeserializer = avroDeserializer2)
  //              .matching(expectedKey = "key-01", expectedValue = Balance(1977L))
    }


}