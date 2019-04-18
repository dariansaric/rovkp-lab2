package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.CellWritable;
import util.DEBSFullRecord;

import java.io.IOException;

public class CellMapper extends Mapper<LongWritable, Text, CellWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            DEBSFullRecord parser = new DEBSFullRecord(value.toString());
            CellWritable cellWritable = new CellWritable(parser.getDropoffLongitude(), parser.getDropoffLatitude());
//            context.write(new Text(cellWritable.toString()), new Text(String.valueOf(parser.getTotal())));
            context.write(cellWritable, new Text(String.valueOf(parser.getTotal())));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
