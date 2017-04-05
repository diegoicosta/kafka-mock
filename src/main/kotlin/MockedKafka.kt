package moip.infra

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
class Inputs<K, V> {

    data class Input<K, V>(val key: K, val value: V)

    private val list: MutableList<Input<K, V>> = mutableListOf()
    var topic: String = ""

    fun add(key: K, value: V): Inputs<K, V> {
        list.add(Input(key, value))
        return this
    }

    fun getInputList(): List<Input<K, V>> {
        return list.toList()
    }

    fun serialize(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): Inputs<ByteArray, ByteArray> {
        val byteList: Inputs<ByteArray, ByteArray> = Inputs()

        for (item in this.list) {
            byteList.add(keySerializer.serialize("", item.key), valueSerializer.serialize("", item.value))
        }

        return byteList
    }
}

data class Output<K, V>(val topic: String, val key: Deserializer<K>, val value: Deserializer<V>)


class EmptySinkError(msg: String) : RuntimeException(msg)
class NoMatchResult(msg: String) : RuntimeException(msg)


class Streaming(val topology: TopologyBuilder) {

    fun <K : Any, V : Any> streaming(key: K, value: V): SourceSerializer<K, V> {

        val inputs = Inputs<K, V>()
        inputs.add(key, value)
        return SourceSerializer(inputs, topology)
    }

    fun <K : Any, V : Any> streaming(inputs: Inputs<K, V>): SourceSerializer<K, V> {

        return SourceSerializer(inputs, topology)
    }
}

class SourceSerializer<K : Any, V : Any>(val inputs: Inputs<K, V>, val topology: TopologyBuilder) {

    fun serializedBy(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): Source {
        val byteInput = inputs.serialize(keySerializer, valueSerializer)
        return Source(byteInput, topology)
    }

    fun streaming(key: K, value: V): SourceSerializer<K, V> {
        inputs.add(key, value)
        return this
    }
}


class Source(val input: Inputs<ByteArray, ByteArray>, val topology: TopologyBuilder) {

    fun fromTopic(name: String): DriverCreator {
        input.topic = name
        return DriverCreator(input, topology)
    }

}


class DriverCreator(val input: Inputs<ByteArray, ByteArray>, val topology: TopologyBuilder) {

    val LOG = LoggerFactory.getLogger(DriverCreator::class.java)

    fun toTopic(outTopic: String): StreamDeserializer {
        LOG.info("Creating test driver processor with source topic '{}'", input.topic)

        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mocked-${UUID.randomUUID()}")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val driver = ProcessorTopologyTestDriver(StreamsConfig(props), topology)

        InputProcessor(input, driver).toTopic(input.topic)

        return StreamDeserializer(outTopic,driver)
    }
}


class InputProcessor(val input: Inputs<ByteArray, ByteArray>, val driver: ProcessorTopologyTestDriver?) {

    val LOG = LoggerFactory.getLogger(InputProcessor::class.java)

    fun toTopic(outTopic: String): StreamDeserializer {
        LOG.info("Processing topologyExtractor test driver with source topic '{}'", input.topic)

        for (i in input.getInputList()) {
            driver?.process(input.topic, i.key, i.value)
        }

        return StreamDeserializer(outTopic = outTopic, driver = driver)
    }
}


class StreamDeserializer(val outTopic: String, val driver: ProcessorTopologyTestDriver?) {

    fun <K, V> deserializedBy(keyDeserializer: Deserializer<K>, valueDeserializer: Deserializer<V>): OutputMatcher<K, V> =
            OutputMatcher(driver, Output(outTopic, keyDeserializer, valueDeserializer))

}


class OutputMatcher<K, V>(val driver: ProcessorTopologyTestDriver?, val output: Output<K, V>) {

    val LOG = LoggerFactory.getLogger(OutputMatcher::class.java)

    fun matching(expectedKey: Any, expectedValue: Any): InputProcessor {

        val record = driver?.readOutput(output.topic)

        record ?: throw EmptySinkError("topic '${output.topic}' is empty")
        if (expectedKey != output.key.deserialize(null, record.key()) || expectedValue != output.value.deserialize(null, record.value())) {
            throw NoMatchResult("topic '${output.topic}' has a different key/value")
        }
        LOG.info("Successfully match in topic '${output.topic}'")
        val input = Inputs<ByteArray, ByteArray>()
        input.add(record.key(), record.value())
        input.topic = output.topic
        return InputProcessor(input, driver)
    }

  //  fun toTopic(outTopic: String): StreamDeserializer {
  //      InputProcessor(input, driver)
  //  }


}

class MockedKafka {

    fun apply(topology: TopologyBuilder): Streaming {
        //topologyExtractor.build(1).
       return Streaming(topology)
    }
}
