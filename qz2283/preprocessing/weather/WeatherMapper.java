import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        if (key.get() == 0 || parts.length < 22) return;

        try {
            String fullDatetime = parts[1];
            String hourKey = fullDatetime.substring(0, 13);

            double temp = parseDoubleSafe(parts[2]);
            double humidity = parseDoubleSafe(parts[5]);
            double precip = parseDoubleSafe(parts[6]);
            double snow = parseDoubleSafe(parts[9]);
            double snowdepth = parseDoubleSafe(parts[10]);
            double windspeed = parseDoubleSafe(parts[12]);
            double visibility = parseDoubleSafe(parts[16]);

            String precipType = cleanString(parts[8]);
            String windDir = cleanString(parts[13]);
            String conditions = cleanString(parts[21]);

            String outputValue = String.format(
                "%.2f|%.2f|%.3f|%.2f|%.2f|%.2f|%.2f|%s|%s|%s|1",
                temp, humidity, precip, snow, snowdepth, windspeed, visibility, precipType, windDir, conditions
            );
            context.write(new Text(hourKey), new Text(outputValue));
        } catch (Exception e) {
        }
    }

    private double parseDoubleSafe(String val) {
        return val.isEmpty() ? 0.0 : Double.parseDouble(val);
    }

    private String cleanString(String val) {
        if (val.isEmpty()) return "Unknown";
        return val.replace("\"", "").replace(",", ";").trim();
    }
}