package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.SimpleRecordTuple;
import util.TripTimeTuple;

public class TripTimeMapper extends Mapper<LongWritable, Text, Text, TripTimeTuple> {
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        System.out.println("test");

        if (key.get() > 0) {
            String record = value.toString();
            try {
                SimpleRecordTuple tuple = SimpleRecordTuple.parse(record);
                TripTimeTuple timeTuple = new TripTimeTuple(tuple.getTripTimeInSecs());

                context.write(new Text(tuple.getMedallion()), timeTuple);
            } catch (Exception e) {
                System.out.println("greška pri mapiranju " + record);
                e.printStackTrace();
            }
        }
    }
}
