package mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.CellWritable;

import java.io.IOException;
import java.util.StringJoiner;

public class CellReducer extends Reducer<CellWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(CellWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long noFares = 0;
        double sum = 0;
        for (Text v : values) {
            noFares++;
            sum += Double.parseDouble(v.toString());
        }

        context.write(NullWritable.get(),
                new Text(
                        new StringJoiner(",")
                                .add(key.toString())
                                .add(String.valueOf(noFares))
                                .add(String.valueOf(sum)).toString()));
    }
}
