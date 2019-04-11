package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.RecordTuple;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text, RecordTuple, Text, RecordTuple> {

    @Override
    protected void reduce(Text key, Iterable<RecordTuple> values, Context context) throws IOException, InterruptedException {
        for (RecordTuple r : values) {
            context.write(key, r);
        }
    }
}
