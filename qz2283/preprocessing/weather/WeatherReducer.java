import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.Map;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        double tempSum = 0.0;
        double humiditySum = 0.0;
        double precipSum = 0.0;
        double snowSum = 0.0;
        double snowDepthSum = 0.0;
        double windSum = 0.0;
        double visibilitySum = 0.0;
        long totalCount = 0;

        Map<String, Integer> conditionsCount = new HashMap<>();
        Map<String, Integer> precipTypeCount = new HashMap<>();
        Map<String, Integer> windDirCount = new HashMap<>();

        for (Text t : values) {
            String[] parts = t.toString().split("\\|");
            if (parts.length < 11) continue;

            try {
                tempSum += Double.parseDouble(parts[0]);
                humiditySum += Double.parseDouble(parts[1]);
                precipSum += Double.parseDouble(parts[2]);
                snowSum += Double.parseDouble(parts[3]);
                snowDepthSum += Double.parseDouble(parts[4]);
                windSum += Double.parseDouble(parts[5]);
                visibilitySum += Double.parseDouble(parts[6]);
                totalCount += Long.parseLong(parts[10]);

                String pType = parts[7];
                String wDir = parts[8];
                String cond = parts[9];

                precipTypeCount.put(pType, precipTypeCount.getOrDefault(pType, 0) + 1);
                windDirCount.put(wDir, windDirCount.getOrDefault(wDir, 0) + 1);
                conditionsCount.put(cond, conditionsCount.getOrDefault(cond, 0) + 1);
            } catch (NumberFormatException ignored) {
            }
        }

        if (totalCount == 0) return;

        double avgTemp = tempSum / totalCount;
        double avgHumidity = humiditySum / totalCount;
        double avgWind = windSum / totalCount;
        double avgVisibility = visibilitySum / totalCount;

        String dominantPrecipType = findModeAndClean(precipTypeCount);
        String dominantWindDir = findModeAndClean(windDirCount);
        String dominantConditions = findModeAndClean(conditionsCount);

        String outputValue = String.format(
            "%.2f,%.2f,%.3f,%.3f,%.2f,%.2f,%.2f,%s,%s,%s",
            avgTemp, avgHumidity, precipSum,
            snowSum,
            snowDepthSum,
            avgWind, avgVisibility, dominantPrecipType, dominantWindDir, dominantConditions
        );
        ctx.write(key, new Text(outputValue));
    }

    private String findModeAndClean(Map<String, Integer> counts) {
        String mode = "Unknown";
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mode = entry.getKey();
            }
        }
        return mode.replace("\"", "")
                .replace(",", ";")
                .trim();
    }
}