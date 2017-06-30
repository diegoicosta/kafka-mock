import moip.mockedk.MetaTopology
import moip.mockedk.MockedKafka
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Created by diegoicosta on 05/03/17.
 */


class TopologyExtractorTest {


    @Test
    fun simpleTopoTest() {

        val builder = KStreamBuilder()
        val stringSerde = Serdes.StringSerde()
        val intSerde = Serdes.Integer()

        //@formatter:off
        builder.stream<String, Int>(stringSerde, intSerde, "topic-01")
                .map { key, value -> KeyValue(value, key)  }
                .to(intSerde, stringSerde, "topic-02")
        //@formatter:on

        val extractor = MetaTopology(builder)

        assertEquals("topic-02", extractor.sinkTopic)
        assertEquals("topic-01", extractor.sourceTopic)

    }


    @Test
    fun testes() {
        val builder = KStreamBuilder()
        val stringSerde = Serdes.StringSerde()
        val intSerde = Serdes.Integer()

        //@formatter:off
        builder.stream<String, Int>(stringSerde, intSerde, "topic-01")
                .map { key, value -> KeyValue(value, key)  }
                .to(intSerde, stringSerde, "topic-02")
        //@formatter:on


        MockedKafka()
                .apply(builder)
                .input("key-01", 1000).input("key-02", 2000)
                .serializedBy(keySerializer = StringSerializer(), valueSerializer = IntegerSerializer())
                .output(1000, "key-01").output(2000, "key-02")
                .deserializedBy(keyDeserializer = IntegerDeserializer(), valueDeserializer = StringDeserializer())

    }


}