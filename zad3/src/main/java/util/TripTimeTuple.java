package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

public class TripTimeTuple implements Writable {
    private long sumTripTime;
    private long minTripTime;
    private long maxTripTime;

    public TripTimeTuple(long sum, long min, long max) {
        sumTripTime = sum;
        minTripTime = min;
        maxTripTime = max;
    }

    public TripTimeTuple(long init) {
        this(init, init, init);
    }

    public TripTimeTuple() {

    }


    public long getSumTripTime() {
        return sumTripTime;
    }

    public void setSumTripTime(long sumTripTime) {
        this.sumTripTime = sumTripTime;
    }

    public long getMinTripTime() {
        return minTripTime;
    }

    public void setMinTripTime(long minTripTime) {
        this.minTripTime = minTripTime;
    }

    public long getMaxTripTime() {
        return maxTripTime;
    }

    public void setMaxTripTime(long maxTripTime) {
        this.maxTripTime = maxTripTime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(sumTripTime);
        dataOutput.writeLong(minTripTime);
        dataOutput.writeLong(maxTripTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumTripTime = dataInput.readLong();
        minTripTime = dataInput.readLong();
        maxTripTime = dataInput.readLong();
    }

    @Override
    public String toString() {
//        return "TripTimeTuple{" +
//                "sumTripTime=" + sumTripTime +
//                ", minTripTime=" + minTripTime +
//                ", maxTripTime=" + maxTripTime +
//                '}';
        return new StringJoiner(",")
                .add(Long.toString(sumTripTime))
                .add(Long.toString(minTripTime))
                .add(Long.toString(maxTripTime))
                .toString();
    }
}
