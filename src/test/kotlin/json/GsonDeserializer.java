package json;

import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * From @beckenbauer
 * Created by allan on 12/09/16.
 */
public class GsonDeserializer<T> implements Deserializer<T> {

    private GsonBuilder builder;
    private Class<T> cls;

    public GsonDeserializer(final Class<T> cls) {
        this();
        this.cls = cls;
    }

    public GsonDeserializer() {
        builder = new GsonBuilder();
        builder.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerde())
               .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerde())
               .registerTypeAdapter(LocalDate.class, new LocalDateSerde());
    }


    @Override
    public void configure(final Map map, final boolean b) {

    }

    @Override
    public T deserialize(final String s, final byte[] bytes) {
        return builder.create().fromJson(new String(bytes), cls);
    }

    @Override
    public void close() {

    }
}
