package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import util.RecordTuple;

public class LocationPassengerPartitioner extends Partitioner<Text, RecordTuple> {
    private static final int LOCATION_PARTITIONER_OFFSET = 3;
    private static final double[] INNER_CITY_BOUND1 = new double[]{-74, 40.8};
    private static final double[] INNER_CITY_BOUND2 = new double[]{-73.95, 40.75};

    @Override
    public int getPartition(Text text, RecordTuple recordTuple, int i) {
        return partitionByPassengerCount(recordTuple.getRecord().getPassengerCount())
                + (isInInnerCity(recordTuple) ? 0 : LOCATION_PARTITIONER_OFFSET);

    }

    private int partitionByPassengerCount(int passengerCount) {
        switch (passengerCount) {
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

    private boolean isInInnerCity(RecordTuple recordTuple) {
        double[] pickup = new double[]{recordTuple.getRecord().getPickupLongitude(),
                recordTuple.getRecord().getPickupLatitude()};
        double[] dropoff = new double[]{recordTuple.getRecord().getDropoffLongitude(),
                recordTuple.getRecord().getDropoffLatitude()};

        return pickup[0] >= INNER_CITY_BOUND1[0] &&
                pickup[0] <= INNER_CITY_BOUND2[0] &&
                pickup[1] >= INNER_CITY_BOUND1[1] &&
                pickup[1] <= INNER_CITY_BOUND2[1] &&
                dropoff[0] >= INNER_CITY_BOUND1[0] &&
                dropoff[0] <= INNER_CITY_BOUND2[0] &&
                dropoff[1] >= INNER_CITY_BOUND1[1] &&
                dropoff[1] <= INNER_CITY_BOUND2[1];
    }
}
