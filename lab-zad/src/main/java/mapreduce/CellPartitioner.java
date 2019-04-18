package mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import util.CellWritable;

public class CellPartitioner extends Partitioner<CellWritable, DoubleWritable> {
    private static final int NO_COLS = 150;

    @Override
    public int getPartition(CellWritable cellWritable, DoubleWritable doubleWritable, int i) {
        return cellWritable.getX() * NO_COLS + cellWritable.getY();
    }
}
