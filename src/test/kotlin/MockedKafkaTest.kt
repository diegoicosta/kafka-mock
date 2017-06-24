package moip.mockedk

import avro.Balance
import avro.SpecificAvroSerde
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import json.Entry
import json.GenericJsonSerde
import json.GsonSerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.junit.Ignore
import org.junit.Test
import java.util.*


/**
 * Created by diegoicosta on 28/10/16.
 */
class MockedKafkaTest {

    val avroSerde = createAvroSerde()



@Test
@Ignore(value = "avro Byte[] comparison is failing even when objects have the same value")
fun testSimpleAvroTopologyMocked() {

  val avroSerializer = avroSerde.serializer()

  MockedKafka()
          .apply(avroSimpleTopology())
          .input(key = "key-01", value = Balance(1977L))
          .serializedBy(StringSerializer(), avroSerializer)
          .output(key = "key-01", value = Balance(1977L))
          .serializedBy(StringSerializer(), avroSerializer)
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


    private fun createAvroSerde(): SpecificAvroSerde<Balance> {

        val schemaRegistry = MockSchemaRegistryClient()

        val specificDeserializerProps = HashMap<String, String>()

        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://fake-url")
        specificDeserializerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")

        val serde = SpecificAvroSerde<Balance>(schemaRegistry, specificDeserializerProps)
        return serde
    }

    private fun simplesTopologyBuilder(): TopologyBuilder {
        val builder = KStreamBuilder()
        val serde = StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .to(serde, serde, "topic-02")
        //@formatter:on

        return builder
    }



    private fun jsonSimpleTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val keySerde = StringSerde()
        val entrySerde = GenericJsonSerde<Entry>(Entry::class.java)

        //@formatter:off
        builder.stream<String, Entry>(keySerde, entrySerde, "topic-01")
                .to(keySerde, entrySerde, "topic-02")
        //@formatter:on            

        return builder
    }

    private fun chainedTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val serde = StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .filter({ k, _ -> k != "NIE" })
                .through(serde, serde, "topic-02")
                .to(serde, serde, "yyy")
        //@formatter:on

        return builder
    }

    private fun stateLessMapTopology(): TopologyBuilder {

        val builder = KStreamBuilder()
        val serde = StringSerde()

        //@formatter:off
        builder.stream<String, String>(serde, serde, "topic-01")
                .map { key, _ -> org.apache.kafka.streams.KeyValue(key, 1) }
                .to(serde, Serdes.Integer(), "topic-02")
        //@formatter:on

        return builder
    }

    @Test
    @Ignore(value = "for some reason, when using 'through', the test fails. To be understood")
    fun oneInputsChainedTopologyTest() {
        MockedKafka()
                .apply(chainedTopology())
                .input(key = "key-01", value = "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
                .output("key-01", "value-01")
                .serializedBy(StringSerializer(), StringSerializer())

    }

    @Test
    fun oneInputsSimpleTopologyTest() {
        MockedKafka()
                .apply(simplesTopologyBuilder())
                .input(key = "key-01", value = "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
                .output("key-01", "value-01")
                .serializedBy(StringSerializer(), StringSerializer())
    }

    @Test
    fun multipleInputsSimpleTopologyTest() {
        MockedKafka()
                .apply(simplesTopologyBuilder())
                .input(key = "key-01", value = "value-01").input("key-02", "value-02")
                .serializedBy(StringSerializer(), StringSerializer())
                .output("key-01", "value-01").output("key-02", "value-02")
                .serializedBy(StringSerializer(), StringSerializer())
    }

    @Test
    fun mapValueTopologyTest() {
        MockedKafka()
                .apply(stateLessMapTopology())
                .input(key = "key-01", value = "value-01").input("key-02", "value-02")
                .serializedBy(StringSerializer(), StringSerializer())
                .output("key-01", 1)
                .serializedBy(StringSerializer(), IntegerSerializer())
    }

    @Test
    fun jsonTopologyTest() {
        val entrySer = GsonSerializer<Entry>()
        val entry = Entry(account = "MPA-001", id = "ENT-001", fee = 3L, amount = 133L)

        MockedKafka()
                .apply(jsonSimpleTopology())
                .input(key = "key-01", value = entry)
                .serializedBy(StringSerializer(), entrySer)
                .output("key-01", entry)
                .serializedBy(StringSerializer(), entrySer)
    }

}