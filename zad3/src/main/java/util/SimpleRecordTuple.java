package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

public class SimpleRecordTuple implements Writable {
    private String medallion;
    private double[] pickup = new double[2];
    private double[] dropoff = new double[2];
    private int passengerCount;
    private long tripTimeInSecs;

    public SimpleRecordTuple() {
    }

    public SimpleRecordTuple(DEBSFullRecord record) {
        pickup[0] = record.getPickupLongitude();
        pickup[1] = record.getPickupLatitude();
        dropoff[0] = record.getDropoffLongitude();
        dropoff[1] = record.getDropoffLatitude();
        passengerCount = record.getPassengerCount();
        tripTimeInSecs = record.getTripTimeInSecs();
    }

    public static SimpleRecordTuple parse(String record) {
        String[] splitted = record.split("\\s+");
        SimpleRecordTuple tuple = new SimpleRecordTuple();
        tuple.setMedallion(splitted[0]);

        splitted = splitted[1].split(",");
        tuple.setPickup(new double[]{Double.parseDouble(splitted[0]), Double.parseDouble(splitted[1])});
        tuple.setDropoff(new double[]{Double.parseDouble(splitted[2]), Double.parseDouble(splitted[3])});
        tuple.setPassengerCount(Integer.parseInt(splitted[4]));
        tuple.setTripTimeInSecs(Long.parseLong(splitted[5]));

        return tuple;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(pickup[0]);
        dataOutput.writeDouble(pickup[1]);
        dataOutput.writeDouble(dropoff[0]);
        dataOutput.writeDouble(dropoff[1]);
        dataOutput.writeInt(passengerCount);
        dataOutput.writeLong(tripTimeInSecs);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pickup[0] = dataInput.readDouble();
        pickup[1] = dataInput.readDouble();
        dropoff[0] = dataInput.readDouble();
        dropoff[1] = dataInput.readDouble();
        passengerCount = dataInput.readInt();
        tripTimeInSecs = dataInput.readLong();
    }

    @Override
    public String toString() {
//        return "SimpleRecordTuple{" +
//                "pickup=" + Arrays.toString(pickup) +
//                ", dropoff=" + Arrays.toString(dropoff) +
//                ", passengerCount=" + passengerCount +
//                ", sumTripTime=" + sumTripTime +
//                ", minTripTime=" + minTripTime +
//                ", maxTripTime=" + maxTripTime +
//                '}';
        return new StringJoiner(",")
                .add(Double.toString(pickup[0])).add(Double.toString(pickup[1]))
                .add(Double.toString(dropoff[0])).add(Double.toString(dropoff[1]))
                .add(Integer.toString(passengerCount)).add(Long.toString(tripTimeInSecs))
                .toString();
    }

    public long getTripTimeInSecs() {
        return tripTimeInSecs;
    }

    private void setTripTimeInSecs(long tripTimeInSecs) {
        this.tripTimeInSecs = tripTimeInSecs;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    private void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
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


    private void setPickup(double[] pickup) {
        this.pickup = pickup;
    }

    private void setDropoff(double[] dropoff) {
        this.dropoff = dropoff;
    }

    public String getMedallion() {
        return medallion;
    }

    private void setMedallion(String medallion) {
        this.medallion = medallion;
    }
}
