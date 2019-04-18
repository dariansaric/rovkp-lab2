package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DEBSFullRecord;

public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final double[] INNER_CITY_BOUND1 = new double[]{-74.913585, 41.474937};
    private static final double[] INNER_CITY_BOUND2 = new double[]{-73.117785, 40.1274702};

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String record = value.toString();

        try {
            DEBSFullRecord parser = new DEBSFullRecord(record);
            if (matchesConstraints(parser)) {
                context.write(NullWritable.get(), value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private boolean matchesConstraints(DEBSFullRecord parser) {
        if (parser.getTotal() <= 0) {
            return false;
        }

        return parser.getPickupLongitude() >= INNER_CITY_BOUND1[0] &&
                parser.getPickupLongitude() <= INNER_CITY_BOUND2[0] &&
                parser.getPickupLatitude() <= INNER_CITY_BOUND1[1] &&
                parser.getPickupLatitude() >= INNER_CITY_BOUND2[1] &&
                parser.getDropoffLongitude() >= INNER_CITY_BOUND1[0] &&
                parser.getDropoffLongitude() <= INNER_CITY_BOUND2[0] &&
                parser.getDropoffLatitude() <= INNER_CITY_BOUND1[1] &&
                parser.getDropoffLatitude() >= INNER_CITY_BOUND2[1];
    }
}
