package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitted = value.toString().split(",");
//        CellWritable cellWritable = new CellWritable(Double.parseDouble(splitted[0]), Double.parseDouble(splitted[1]));
//        int noFares = Integer.parseInt(splitted[2]);
        context.write(new Text(String.valueOf(1)), value);
    }
}
