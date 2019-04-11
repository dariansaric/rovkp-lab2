package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import util.RecordTuple;

public class NoOfPassengersPartitioner extends Partitioner<Text, RecordTuple> {
    @Override
    public int getPartition(Text text, RecordTuple pickupTuple, int i) {
        switch (pickupTuple.getRecord().getPassengerCount()) {
            case 1:
                return 0;
            case 2:
            case 3:
                return 1;
            case 4:
            default:
                return 2;
        }
    }
}
