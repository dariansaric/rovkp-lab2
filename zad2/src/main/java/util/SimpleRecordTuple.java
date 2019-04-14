package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class SimpleRecordTuple implements Writable {
    private double[] pickup = new double[2];
    private double[] dropoff = new double[2];
    private int passengerCount;
    private long sumTripTime;
    private long minTripTime;
    private long maxTripTime;

    public SimpleRecordTuple() {
    }

    public SimpleRecordTuple(DEBSFullRecord record) {
        pickup[0] = record.getPickupLongitude();
        pickup[1] = record.getPickupLatitude();
        dropoff[0] = record.getDropoffLongitude();
        dropoff[1] = record.getDropoffLatitude();
        passengerCount = record.getPassengerCount();
        sumTripTime = record.getTripTimeInSecs();
        minTripTime = sumTripTime;
        maxTripTime = sumTripTime;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(pickup[0]);
        dataOutput.writeDouble(pickup[1]);
        dataOutput.writeDouble(dropoff[0]);
        dataOutput.writeDouble(dropoff[1]);
        dataOutput.writeInt(passengerCount);
        dataOutput.writeLong(sumTripTime);
        dataOutput.writeLong(minTripTime);
        dataOutput.writeLong(maxTripTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pickup[0] = dataInput.readDouble();
        pickup[1] = dataInput.readDouble();
        dropoff[0] = dataInput.readDouble();
        dropoff[1] = dataInput.readDouble();
        passengerCount = dataInput.readInt();
        sumTripTime = dataInput.readLong();
        minTripTime = dataInput.readLong();
        maxTripTime = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "SimpleRecordTuple{" +
                "pickup=" + Arrays.toString(pickup) +
                ", dropoff=" + Arrays.toString(dropoff) +
                ", passengerCount=" + passengerCount +
                '}';
    }

    public long getSumTripTime() {
        return sumTripTime;
    }

    public long getMinTripTime() {
        return minTripTime;
    }

    public long getMaxTripTime() {
        return maxTripTime;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public double getPickupLongitude() {
        return pickup[0];
    }

    public double getPickupLatitude() {
        return pickup[1];
    }

    public double getDropoffLongitude() {
        return pickup[0];
    }

    public double getDropoffLatitude() {
        return dropoff[1];
    }
}
