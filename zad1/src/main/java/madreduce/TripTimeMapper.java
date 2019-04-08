package madreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DEBSRecordParser;
import util.TripTimeTuple;

import java.io.IOException;

public class TripTimeMapper extends Mapper<LongWritable, Text, Text, TripTimeTuple> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() > 0) {
            String record = value.toString();
            try {
                DEBSRecordParser parser = new DEBSRecordParser();
                parser.parse(record);
//                TripTimeTuple tuple = new TripTimeTuple();
//                long tripTime = parser.getTripTimeSecs();
//                tuple.setSumTripTime(tripTime);
//                tuple.setMinTripTime(tripTime);
//                tuple.setMaxTripTime(tripTime);

                context.write(new Text(parser.getMedallion()), new TripTimeTuple(parser.getTripTimeSecs()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        super.map(key, value, context);
    }
}
