package moip.kafkautils.serde;

import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * From @beckenbauer
 * Created by montanha on 10/11/16.
 */
public class LocalDateTimeSerde implements JsonDeserializer<LocalDateTime>, JsonSerializer<LocalDateTime> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    @Override
    public LocalDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        if (isTimestamp(json.getAsString()))
            return FORMATTER.parse(formatTimestamp(json.getAsLong()), LocalDateTime::from);

        return FORMATTER.parse(json.getAsString(), LocalDateTime::from);
    }

    @Override
    public JsonElement serialize(LocalDateTime src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(FORMATTER.format(src));
    }

    public boolean isTimestamp(String json){
        return StringUtils.isNumeric(json);
    }

    public String formatTimestamp(Long json){
        Date date = new Date(json);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        return dateFormat.format(date);
    }
}
