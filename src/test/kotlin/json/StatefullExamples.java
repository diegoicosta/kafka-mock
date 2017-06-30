package json;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * Created by diegoicosta on 28/02/17.
 */
public class StatefullExamples {

    private TopologyBuilder avroCountTopology() {

        KStreamBuilder builder = new KStreamBuilder();
        Serdes.StringSerde keySerde = new Serdes.StringSerde();
        GenericJsonSerde<Entry> entrySerde = new GenericJsonSerde<>();
        GenericJsonSerde<EntrySum> entrySumSerde = new GenericJsonSerde<>();

        //@formatter:off
        builder.stream(keySerde, entrySerde, "topic-01")
                .map((key, value) -> new KeyValue<String, Entry>(value.getAccount(), value))
                .groupByKey()
                .aggregate(()-> new EntrySum(null,0L),
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value.getAmount());
                            return aggregate;
                         },
                        entrySumSerde,"balance-store");
        //@formatter:on

        return builder;
    }

}
