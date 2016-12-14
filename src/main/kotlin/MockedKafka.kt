package moip.beckenbauer.infra

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

data class Input(val topic: String = "", val key: ByteArray, val value: ByteArray)

data class Output<K, V>(val topic: String, val key: Deserializer<K>, val value: Deserializer<V>)


class EmptySinkError(msg: String) : RuntimeException(msg)
class NoMatchResult(msg: String) : RuntimeException(msg)


class Streaming(val topology: TopologyBuilder) {

    fun <K : Any, V : Any> streaming(key: K, value: V): SourceSerializer<K, V> = SourceSerializer(key, value, topology)

}

class SourceSerializer<K : Any, V : Any>(val key: K, val value: V, val topology: TopologyBuilder) {

    fun serializedBy(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): Source {

        return Source(Input(key = keySerializer.serialize(null, key), value = valueSerializer.serialize(null, value)), topology)
    }
}

class Source(val input: Input, val topology: TopologyBuilder) {

    fun fromTopic(name: String): DriverCreator =
            DriverCreator(input.copy(name, input.key, input.value), topology)

}

class InputProcessor(val input: Input, val driver: ProcessorTopologyTestDriver?) {

    val LOG = LoggerFactory.getLogger(InputProcessor::class.java)

    fun toTopic(outTopic: String): moip.beckenbauer.infra.StreamDeserializer {
        LOG.info("Processing topology test driver with source topic '{}'", input.topic)

        driver?.process(input.topic, input.key, input.value)

        return StreamDeserializer(outTopic = outTopic, driver = driver)
    }
}

class DriverCreator(val input: Input, val topology: TopologyBuilder) {

    val LOG = LoggerFactory.getLogger(DriverCreator::class.java)

    fun toTopic(outTopic: String): moip.beckenbauer.infra.StreamDeserializer {
        LOG.info("Creating test driver processor with source topic '{}'", input.topic)

        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mocked-${UUID.randomUUID()}")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val driver = ProcessorTopologyTestDriver(StreamsConfig(props), topology)

        InputProcessor(input, driver).toTopic(input.topic)

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
        val input = Input(topic = output.topic, key = record.key(), value = record.value())
        return InputProcessor(input, driver)
    }

}

class MockedKafka {

    fun apply(topology: TopologyBuilder): Streaming =
            Streaming(topology)
}
