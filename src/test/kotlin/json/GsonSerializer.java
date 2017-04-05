package json;

import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * From @beckenbauer
 * Created by allan on 12/09/16.
 */
public class GsonSerializer<T> implements Serializer<T> {

    private GsonBuilder builder;

    public GsonSerializer() {
        builder = new GsonBuilder();
        builder.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerde())
               .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerde())
               .registerTypeAdapter(LocalDate.class, new LocalDateSerde());
    }

    @Override
    public void configure(final Map map, final boolean b) {
    }

    @Override
    public byte[] serialize(final String s, final T o) {
        return builder.create().toJson(o, o.getClass()).getBytes();
    }

    @Override
    public void close() {

    }
}
