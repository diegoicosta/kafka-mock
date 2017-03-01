package moip.kafkautils.serde;

import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Created by diegoicosta on 08/12/16.
 */
public class GenericJsonSerde<T> implements Serde<T> {
    private Class<T> cls;
    private GsonBuilder builder;

    public GenericJsonSerde() {
        builder = new GsonBuilder();
        builder.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerde())
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerde())
                .registerTypeAdapter(LocalDate.class, new LocalDateSerde());
    }

    public GenericJsonSerde(Class<T> cls) {
        this();
        this.cls = cls;
    }

    @Override
    public void configure(Map map, boolean b) {
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new GsonSerializer<T>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new GsonDeserializer<T>(cls);
    }
}
