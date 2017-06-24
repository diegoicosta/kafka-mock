package moip.mockedk

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Created by diegoicosta on 12/10/16.
 */
class DataStream<K, V> {

    data class KeyValue<K, V>(val key: K, val value: V)

    private val list: MutableList<KeyValue<K, V>> = mutableListOf()

    fun add(key: K, value: V): DataStream<K, V> {
        list.add(KeyValue(key, value))
        return this
    }

    fun getStreamList(): List<KeyValue<K, V>> {
        return list.toList()
    }

    fun serialize(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): DataStream<ByteArray, ByteArray> {
        val byteList: DataStream<ByteArray, ByteArray> = DataStream()

        for (item in this.list) {
            byteList.add(keySerializer.serialize("", item.key), valueSerializer.serialize("", item.value))
        }

        return byteList
    }
}

class EmptySinkError(msg: String) : RuntimeException(msg)
class NoMatchResult(msg: String) : RuntimeException(msg)


class InputStream(private val topologyExtractor: MetaTopology) {

    fun <K : Any, V : Any> input(key: K, value: V): SourceSerializer<K, V> {

        val inputs = DataStream<K, V>().add(key, value)

        return SourceSerializer(inputs, topologyExtractor)
    }
}

class SourceSerializer<K : Any, V : Any>(private val inputs: DataStream<K, V>, private val topology: MetaTopology) {

    fun serializedBy(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): OutputStream {
        val byteInput = inputs.serialize(keySerializer, valueSerializer)

        return OutputStream(topology, byteInput)
    }

    fun input(key: K, value: V): SourceSerializer<K, V> {
        inputs.add(key, value)
        return this
    }
}

class OutputStream(private val topologyExtractor: MetaTopology, private val input: DataStream<ByteArray, ByteArray>) {

    val LOG = LoggerFactory.getLogger(OutputStream::class.java)

    fun <K : Any, V : Any> output(key: K, value: V): SinkSerializer<K, V> {
        val output = DataStream<K, V>().add(key, value)
        return SinkSerializer(input, output, topologyExtractor)
    }

}


class SinkSerializer<K, V>(private val input: DataStream<ByteArray, ByteArray>, private val output: DataStream<K, V>, private val topology: MetaTopology) {

    fun serializedBy(keySerializer: Serializer<K>, valueSerializer: Serializer<V>) {
        val outputBytes = output.serialize(keySerializer, valueSerializer)


        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mocked-${UUID.randomUUID()}")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val driver = ProcessorTopologyTestDriver(StreamsConfig(props), topology.topology)

        for (i in input.getStreamList()) {
            driver.process(topology.sourceTopic, i.key, i.value)
        }

        for (out in outputBytes.getStreamList()) {
            val record = driver.readOutput(topology.sinkTopic)

            record ?: throw EmptySinkError("topic '${topology.sinkTopic}' is empty")
            if (!Arrays.equals(out.value, record.value())
                    || !Arrays.equals(out.key, record.key())
                    ) {

                //print("diff: ${String(out.value)} - ${String(record.value())}")
               // print("diff: ${(out.value)} - ${(record.value())}")
                throw NoMatchResult("topic '${topology.sinkTopic}' has a different key/value")
            }
        }

    }


    fun output(key: K, value: V): SinkSerializer<K, V> {
        output.add(key, value)
        return this
    }

}

data class MetaTopology(val topology: TopologyBuilder) {
    val sourceTopic: String = topology.sourceTopics().last()
    val sinkTopic: String = topology.build(null).sinkTopics().last()
}


class MockedKafka {

    fun apply(topology: TopologyBuilder): InputStream {
        val extractor = MetaTopology(topology)
        return InputStream(extractor)
    }
}
