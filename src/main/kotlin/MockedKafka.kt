package moip.mockedk

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Created by diegoicosta on 12/10/16.
 */
class MockedKafka {

    fun apply(topology: TopologyBuilder): InputStream {
        val extractor = MetaTopology(topology)
        return InputStream(extractor)
    }
}

class InputStream(private val topologyExtractor: MetaTopology) {

    fun <K : Any, V : Any> input(key: K, value: V): SourceSerializer<K, V> {

        val inputs = DataStream<K, V>().add(key, value)

        return SourceSerializer(inputs, topologyExtractor)
    }
}

class SourceSerializer<K : Any, V : Any>(private val inputs: DataStream<K, V>, private val topology: MetaTopology) {

    fun serializedBy(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): OutputStream<K, V> {
        inputs.serialize(keySerializer, valueSerializer)
        return OutputStream(topology, inputs)
    }

    fun input(key: K, value: V): SourceSerializer<K, V> {
        inputs.add(key, value)
        return this
    }
}

class OutputStream<K : Any, V : Any>(private val topologyExtractor: MetaTopology, private val input: DataStream<K, V>) {

    val LOG = LoggerFactory.getLogger(OutputStream::class.java)

    fun <K2 : Any, V2 : Any> output(key: K2, value: V2): SinkSerializer<K, V, K2, V2> {
        val output = DataStream<K2, V2>().add(key, value)
        return SinkSerializer(input, output, topologyExtractor)
    }

}

class DataStream<K, V> {

    data class KeyValue<K, V>(val key: K, val value: V)

    private val list: MutableList<KeyValue<K, V>> = mutableListOf()
    private val byteList: MutableList<KeyValue<ByteArray, ByteArray>> = mutableListOf()

    fun add(key: K, value: V): DataStream<K, V> {
        list.add(KeyValue(key, value))
        return this
    }

    fun geSerializedList(): List<KeyValue<ByteArray, ByteArray>> {
        return byteList.toList()
    }

    fun geDeserializedList(): List<KeyValue<K, V>> {
        return list.toList()
    }

    fun serialize(keySerializer: Serializer<K>, valueSerializer: Serializer<V>) {
        for (item in this.list) {
            byteList.add(KeyValue(keySerializer.serialize("", item.key), valueSerializer.serialize("", item.value)))
        }
    }

}

class EmptySinkError(msg: String) : RuntimeException(msg)
class NoMatchResult(msg: String) : RuntimeException(msg)

class SinkSerializer<K1, V1, K2, V2>(private val input: DataStream<K1, V1>, private val output: DataStream<K2, V2>, private val topology: MetaTopology) {

    fun deserializedBy(keyDeserializer: Deserializer<K2>, valueDeserializer: Deserializer<V2>) {

        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mocked-${UUID.randomUUID()}")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val driver = ProcessorTopologyTestDriver(StreamsConfig(props), topology.topology)

        for (i in input.geSerializedList()) {
            driver.process(topology.sourceTopic, i.key, i.value)
        }

        for (out in output.geDeserializedList()) {
            val record = driver.readOutput(topology.sinkTopic)

            record ?: throw EmptySinkError("topic '${topology.sinkTopic}' is empty")

            val recOutKey = keyDeserializer.deserialize("", record.key())
            val recOutVal = valueDeserializer.deserialize("", record.value())

            if ( recOutKey != out.key || recOutVal != out.value) {
                throw NoMatchResult("topic '${topology.sinkTopic}' has a different key/value")
            }

        }

    }


    fun output(key: K2, value: V2): SinkSerializer<K1, V1, K2, V2> {
        output.add(key, value)
        return this
    }

}

data class MetaTopology(val topology: TopologyBuilder) {
    val sourceTopic = topology.build(null).sourceTopics().last()
    //    val sourceTopic: String = topology.sourceTopics().last()  older k-stream version
    val sinkTopic: String = topology.build(null).sinkTopics().last()
}

