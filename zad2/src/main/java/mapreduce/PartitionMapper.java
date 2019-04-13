package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DEBSFullRecord;
import util.SimpleRecordTuple;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text, Text, SimpleRecordTuple> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            try {
                DEBSFullRecord parser = new DEBSFullRecord(value.toString());
                context.write(new Text(parser.getMedallion()), new SimpleRecordTuple(parser));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
