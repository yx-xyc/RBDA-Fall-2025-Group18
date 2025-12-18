import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (key.get() == 0 || line.startsWith("\"ARREST_KEY\"")) return;

        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        if (parts.length < 18) return;

        try {
            String arrestDate = parts[1].replace("\"", "").trim();
            if (arrestDate.isEmpty()) return;

            String[] dateParts = arrestDate.split("/");
            if (dateParts.length != 3) return;

            String dateKey = dateParts[2] + "-" + dateParts[0] + "-" + dateParts[1];

            String offenseDesc = parts[5].trim();
            if (offenseDesc.isEmpty()) offenseDesc = "UNKNOWN";
            offenseDesc = offenseDesc.replace("\"", "").replace(",", ";").trim();

            String boro = parts[8].replace("\"", "").trim();
            if (boro.isEmpty()) boro = "U";

            double latitude = parseDoubleSafe(parts[16]);
            double longitude = parseDoubleSafe(parts[17]);

            if (latitude == 0.0 || longitude == 0.0) return;

            // Create output value with all arrest details
            String outputValue = String.format("%s,%f,%f", offenseDesc, latitude, longitude);
            
            // Use date as key
            context.write(new Text(dateKey), new Text(outputValue));

        } catch (Exception e) {
        }
    }

    private double parseDoubleSafe(String val) {
        if (val == null) return 0.0;
        val = val.replace("\"", "").trim();
        if (val.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(val);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
