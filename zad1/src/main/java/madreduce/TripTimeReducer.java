package madreduce;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.TripTimeTuple;

import java.io.IOException;

import static java.lang.Long.max;
import static java.lang.Long.min;

public class TripTimeReducer extends Reducer<Text, TripTimeTuple, Text, TripTimeTuple> {
    @Override
    protected void reduce(Text key, Iterable<TripTimeTuple> values, Context context) throws IOException, InterruptedException {
        long sumTripTime = 0L;
        long minTripTime = Long.MAX_VALUE;
        long maxTripTime = Long.MIN_VALUE; //valjda nece bit negativnog inace Long.MIN_VALUE

        for (TripTimeTuple t : values) {
            sumTripTime += t.getSumTripTime();
            minTripTime = min(minTripTime, t.getMinTripTime());
            maxTripTime = max(maxTripTime, t.getMaxTripTime());
        }
        LogFactory.getLog(TripTimeReducer.class).debug("Test na reduceru");
        context.write(key, new TripTimeTuple(sumTripTime, minTripTime, maxTripTime));
    }
}