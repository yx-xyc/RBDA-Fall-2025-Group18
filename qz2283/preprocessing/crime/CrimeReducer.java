import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Pass through each individual arrest record
        // This preserves exact coordinates for joining with weather data later
        for (Text val : values) {
            context.write(key, val);
        }
    }
}
