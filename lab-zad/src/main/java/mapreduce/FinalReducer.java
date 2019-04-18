package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.CellWritable;

import java.io.IOException;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxEntry = "";
        String minEntry = "";

        for (Text v : values) {
            String[] splitted = v.toString().split(",");
            CellWritable cellWritable = new CellWritable(Double.parseDouble(splitted[0]), Double.parseDouble(splitted[1]));
            int noFares = Integer.parseInt(splitted[2]);
            if (maxEntry.isEmpty()) {
                maxEntry = v.toString();
            } else {
                String[] splitted1 = maxEntry.split(",");
                if (Integer.parseInt(splitted1[2]) < noFares) {
                    maxEntry = v.toString();
                }
            }
            if (minEntry.isEmpty()) {
                minEntry = v.toString();
            } else {
                String[] splitted1 = minEntry.split(",");
                if (Integer.parseInt(splitted1[2]) > noFares) {
                    minEntry = v.toString();
                }
            }
        }
        String[] splitted = maxEntry.split(",");
        String[] splitted1 = minEntry.split(",");
        context.write(new Text(maxEntry), new Text(splitted[0] + "," + splitted[1]));
        context.write(new Text(minEntry), new Text(new CellWritable(Double.parseDouble(splitted1[0]), Double.parseDouble(splitted1[1])).toString()));
    }
}
