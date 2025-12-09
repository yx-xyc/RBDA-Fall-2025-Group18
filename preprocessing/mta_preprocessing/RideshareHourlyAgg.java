import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * MapReduce Job: Rideshare Hourly Aggregation
 * - Aggregates rideshare trip data by pickup hour and location
 * - Calculates trip counts, average costs, durations, and shared ride metrics
 * - Filters for 2024 data to match MTA pipeline
 */
public class RideshareHourlyAgg {

    public enum AggregationCounters {
        TOTAL_RECORDS,
        VALID_RECORDS,
        FILTERED_WRONG_YEAR,
        INVALID_TIMESTAMP,
        INVALID_LOCATION,
        INVALID_COST_DATA,
        INVALID_DURATION
    }

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, Text> {

        // ISO 8601 format: 2024-01-01T00:00:00
        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        private SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header line
            if (line.startsWith("hvfhs_license_num") || line.startsWith("pickup_datetime")) {
                return;
            }

            context.getCounter(AggregationCounters.TOTAL_RECORDS).increment(1);

            // Split CSV (handle quoted fields)
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Expected columns (23 total):
            // 0:hvfhs_license_num, 1:dispatching_base_num, 2:request_datetime,
            // 3:on_scene_datetime, 4:pickup_datetime, 5:dropoff_datetime,
            // 6:PULocationID, 7:DOLocationID, 8:trip_miles, 9:trip_time,
            // 10:base_passenger_fare, 11:tolls, 12:bcf, 13:sales_tax,
            // 14:congestion_surcharge, 15:airport_fee, 16:tips, 17:driver_pay,
            // 18:shared_request_flag, 19:shared_match_flag, 20:access_a_ride_flag,
            // 21:wav_request_flag, 22:wav_match_flag

            if (fields.length < 23) {
                context.getCounter(AggregationCounters.INVALID_TIMESTAMP).increment(1);
                return;
            }

            // Clean quotes from fields
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].trim().replaceAll("^\"|\"$", "");
            }

            // 1. Parse and validate pickup_datetime - filter for 2024 only
            String pickupDatetime = fields[4];
            Date parsedDate;
            String pickupHour;

            try {
                parsedDate = inputDateFormat.parse(pickupDatetime);

                // Check if year is 2024
                int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(parsedDate));
                if (year != 2024) {
                    context.getCounter(AggregationCounters.FILTERED_WRONG_YEAR).increment(1);
                    return;
                }

                // Normalize to hour level
                pickupHour = outputDateFormat.format(parsedDate);

            } catch (ParseException | NumberFormatException e) {
                context.getCounter(AggregationCounters.INVALID_TIMESTAMP).increment(1);
                return;
            }

            // 2. Validate PULocationID
            String locationId = fields[6];
            if (isNullOrEmpty(locationId)) {
                context.getCounter(AggregationCounters.INVALID_LOCATION).increment(1);
                return;
            }

            // Try to parse as integer to validate
            try {
                Integer.parseInt(locationId);
            } catch (NumberFormatException e) {
                context.getCounter(AggregationCounters.INVALID_LOCATION).increment(1);
                return;
            }

            // 3. Validate and parse cost fields
            String baseFare = fields[10];
            String tolls = fields[11];
            String bcf = fields[12];
            String salesTax = fields[13];
            String congestionSurcharge = fields[14];
            String airportFee = fields[15];
            String tips = fields[16];
            String driverPay = fields[17];

            // Replace null/empty with "0.0"
            baseFare = isNullOrEmpty(baseFare) ? "0.0" : baseFare;
            tolls = isNullOrEmpty(tolls) ? "0.0" : tolls;
            bcf = isNullOrEmpty(bcf) ? "0.0" : bcf;
            salesTax = isNullOrEmpty(salesTax) ? "0.0" : salesTax;
            congestionSurcharge = isNullOrEmpty(congestionSurcharge) ? "0.0" : congestionSurcharge;
            airportFee = isNullOrEmpty(airportFee) ? "0.0" : airportFee;
            tips = isNullOrEmpty(tips) ? "0.0" : tips;
            driverPay = isNullOrEmpty(driverPay) ? "0.0" : driverPay;

            // Validate cost fields are numeric
            try {
                Double.parseDouble(baseFare);
                Double.parseDouble(tolls);
                Double.parseDouble(tips);
                Double.parseDouble(driverPay);
            } catch (NumberFormatException e) {
                context.getCounter(AggregationCounters.INVALID_COST_DATA).increment(1);
                return;
            }

            // 4. Validate and parse trip metrics
            String tripMiles = fields[8];
            String tripTime = fields[9];

            tripMiles = isNullOrEmpty(tripMiles) ? "0.0" : tripMiles;
            tripTime = isNullOrEmpty(tripTime) ? "0.0" : tripTime;

            try {
                Double.parseDouble(tripMiles);
                Double.parseDouble(tripTime);
            } catch (NumberFormatException e) {
                context.getCounter(AggregationCounters.INVALID_DURATION).increment(1);
                return;
            }

            // 5. Parse flag fields
            String sharedRequest = fields[18];
            String sharedMatch = fields[19];
            String wavRequest = fields[21];
            String wavMatch = fields[22];

            // Composite key: pickup_hour|location_id
            String compositeKey = pickupHour + "|" + locationId;

            // Value: all metrics separated by pipe
            // Format: base_fare|tolls|bcf|sales_tax|congestion|airport|tips|driver_pay|
            //         trip_miles|trip_time|shared_req|shared_match|wav_req|wav_match
            StringBuilder valueBuilder = new StringBuilder();
            valueBuilder.append(baseFare).append("|");
            valueBuilder.append(tolls).append("|");
            valueBuilder.append(bcf).append("|");
            valueBuilder.append(salesTax).append("|");
            valueBuilder.append(congestionSurcharge).append("|");
            valueBuilder.append(airportFee).append("|");
            valueBuilder.append(tips).append("|");
            valueBuilder.append(driverPay).append("|");
            valueBuilder.append(tripMiles).append("|");
            valueBuilder.append(tripTime).append("|");
            valueBuilder.append(sharedRequest).append("|");
            valueBuilder.append(sharedMatch).append("|");
            valueBuilder.append(wavRequest).append("|");
            valueBuilder.append(wavMatch);

            context.getCounter(AggregationCounters.VALID_RECORDS).increment(1);
            context.write(new Text(compositeKey), new Text(valueBuilder.toString()));
        }

        private boolean isNullOrEmpty(String str) {
            return str == null || str.trim().isEmpty() ||
                   str.equalsIgnoreCase("null") ||
                   str.equalsIgnoreCase("none") ||
                   str.equalsIgnoreCase("na");
        }
    }

    public static class AggregationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Accumulators
            int tripCount = 0;
            double sumBaseFare = 0.0;
            double sumTolls = 0.0;
            double sumBcf = 0.0;
            double sumSalesTax = 0.0;
            double sumCongestion = 0.0;
            double sumAirport = 0.0;
            double sumTips = 0.0;
            double sumDriverPay = 0.0;
            double sumTripMiles = 0.0;
            double sumTripTime = 0.0;
            int sharedRequestCount = 0;
            int sharedMatchCount = 0;
            int wavRequestCount = 0;
            int wavMatchCount = 0;

            // Process all values for this key
            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                if (parts.length < 14) {
                    continue;
                }

                try {
                    tripCount++;
                    sumBaseFare += Double.parseDouble(parts[0]);
                    sumTolls += Double.parseDouble(parts[1]);
                    sumBcf += Double.parseDouble(parts[2]);
                    sumSalesTax += Double.parseDouble(parts[3]);
                    sumCongestion += Double.parseDouble(parts[4]);
                    sumAirport += Double.parseDouble(parts[5]);
                    sumTips += Double.parseDouble(parts[6]);
                    sumDriverPay += Double.parseDouble(parts[7]);
                    sumTripMiles += Double.parseDouble(parts[8]);
                    sumTripTime += Double.parseDouble(parts[9]);

                    if (parts[10].equalsIgnoreCase("Y")) sharedRequestCount++;
                    if (parts[11].equalsIgnoreCase("Y")) sharedMatchCount++;
                    if (parts[12].equalsIgnoreCase("Y")) wavRequestCount++;
                    if (parts[13].equalsIgnoreCase("Y")) wavMatchCount++;

                } catch (NumberFormatException e) {
                    // Skip invalid values
                    continue;
                }
            }

            if (tripCount == 0) {
                return;
            }

            // Calculate averages
            double avgBaseFare = sumBaseFare / tripCount;
            double avgTolls = sumTolls / tripCount;
            double avgTips = sumTips / tripCount;
            double avgDriverPay = sumDriverPay / tripCount;
            double avgTotalCost = (sumBaseFare + sumTolls + sumBcf + sumSalesTax +
                                   sumCongestion + sumAirport) / tripCount;
            double avgTripDurationSec = sumTripTime / tripCount;
            double avgTripMiles = sumTripMiles / tripCount;
            double sharedMatchPct = (tripCount > 0) ? (100.0 * sharedMatchCount / tripCount) : 0.0;

            // Parse key components
            String[] keyParts = key.toString().split("\\|");
            String pickupHour = keyParts[0];
            String locationId = keyParts[1];

            // Output format (CSV):
            // pickup_hour,pickup_location_id,trip_count,avg_base_fare,avg_tolls,avg_tips,
            // avg_driver_pay,avg_total_cost,avg_trip_duration_sec,avg_trip_miles,
            // shared_request_count,shared_match_count,shared_match_pct,
            // wheelchair_request_count,wheelchair_match_count

            StringBuilder output = new StringBuilder();
            output.append(pickupHour).append(",");
            output.append(locationId).append(",");
            output.append(tripCount).append(",");
            output.append(String.format("%.2f", avgBaseFare)).append(",");
            output.append(String.format("%.2f", avgTolls)).append(",");
            output.append(String.format("%.2f", avgTips)).append(",");
            output.append(String.format("%.2f", avgDriverPay)).append(",");
            output.append(String.format("%.2f", avgTotalCost)).append(",");
            output.append(String.format("%.2f", avgTripDurationSec)).append(",");
            output.append(String.format("%.2f", avgTripMiles)).append(",");
            output.append(sharedRequestCount).append(",");
            output.append(sharedMatchCount).append(",");
            output.append(String.format("%.2f", sharedMatchPct)).append(",");
            output.append(wavRequestCount).append(",");
            output.append(wavMatchCount);

            context.write(null, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RideshareHourlyAgg <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rideshare Hourly Aggregation by Location");

        job.setJarByClass(RideshareHourlyAgg.class);
        job.setMapperClass(AggregationMapper.class);
        job.setReducerClass(AggregationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        // Print aggregation report
        if (success) {
            System.out.println("\n=== AGGREGATION REPORT ===");
            for (Counter counter : job.getCounters().getGroup(AggregationCounters.class.getName())) {
                System.out.println(counter.getDisplayName() + ": " + counter.getValue());
            }
        }

        System.exit(success ? 0 : 1);
    }
}
