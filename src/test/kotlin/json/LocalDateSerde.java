package moip.kafkautils.serde;

import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * From @beckenbauer
 * Created by montanha on 10/11/16.
 */
public class LocalDateSerde implements JsonDeserializer<LocalDate>, JsonSerializer<LocalDate> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public LocalDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        if (isTimestamp(json.getAsString()))
            return FORMATTER.parse(formatTimestamp(json.getAsLong()), LocalDate::from);

        return FORMATTER.parse(json.getAsString(), LocalDate::from);
    }

    @Override
    public JsonElement serialize(LocalDate src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(FORMATTER.format(src));
    }

    public boolean isTimestamp(String json){
        return StringUtils.isNumeric(json);
    }

    public String formatTimestamp(Long json){
        Date date = new Date(json);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

}
