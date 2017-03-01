package moip.kafkautils.serde;

import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * From @beckenbauer
 * Created by diegoicosta on 11/09/16.
 */
public class ZonedDateTimeSerde implements JsonDeserializer<ZonedDateTime>, JsonSerializer<ZonedDateTime> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd\'T\'HH:mm:ss.SSSX");

    @Override
    public ZonedDateTime deserialize(JsonElement json, Type type,
            JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (isTimestamp(json.getAsString()))
            return ZonedDateTime.parse(formatTimestamp(json.getAsLong()), FORMATTER);

        return ZonedDateTime.parse(json.getAsString(), FORMATTER);
    }

    @Override
    public JsonElement serialize(ZonedDateTime zonedDateTime, Type type,
            JsonSerializationContext jsonSerializationContext) {
        return new JsonPrimitive(zonedDateTime.format(FORMATTER));
    }

    public boolean isTimestamp(String json){
        return StringUtils.isNumeric(json);
    }

    public String formatTimestamp(Long json){
        Date date = new Date(json);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSSX");
        return dateFormat.format(date);
    }
}
