package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.SimpleRecordTuple;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text, SimpleRecordTuple, Text, SimpleRecordTuple> {

    @Override
    protected void reduce(Text key, Iterable<SimpleRecordTuple> values, Context context) {
        values.forEach(v -> {
            try {
                context.write(key, v);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
