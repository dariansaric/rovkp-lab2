package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import util.RecordTuple;

public class LocationPartitioner extends Partitioner<Text, RecordTuple> {
    private static final double[] CENTER_EDGE1 = new double[]{-74, 40.8};
    private static final double[] CENTER_EDGE2 = new double[]{-73.95, 40.75};

    @Override
    public int getPartition(Text text, RecordTuple pickupTuple, int i) {
        return 0; //todo koji kurac
    }
}
