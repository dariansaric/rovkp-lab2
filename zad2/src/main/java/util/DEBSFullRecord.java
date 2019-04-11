/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class DEBSFullRecord {
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private final String medallion;
    private final String hackLicense;
    private final String vendorID;
    private final String storeAndFwdFlag;
    private final Date pickup;
    private final Date dropoff;
    private final int passengerCount;
    private final long tripTimeInSecs;
    private final double pickupLongitude;
    private final double pickupLatitude;
    private final double dropoffLongitude;
    private final double dropoffLatitude;

    public DEBSFullRecord(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        hackLicense = splitted[1];
        vendorID = splitted[2];
        storeAndFwdFlag = splitted[3];
        pickup = DATE_FORMATTER.parse(splitted[3]);
        dropoff = DATE_FORMATTER.parse(splitted[4]);
        passengerCount = Integer.parseInt(splitted[5]);
        tripTimeInSecs = Long.parseLong(splitted[6]);
        pickupLongitude = Double.parseDouble(splitted[7]);
        pickupLatitude = Double.parseDouble(splitted[8]);
        dropoffLongitude = Double.parseDouble(splitted[9]);
        dropoffLatitude = Double.parseDouble(splitted[10]);
    }

    public String getMedallion() {
        return medallion;
    }

    public String getHackLicense() {
        return hackLicense;
    }

    public String getVendorID() {
        return vendorID;
    }

    public String getStoreAndFwdFlag() {
        return storeAndFwdFlag;
    }

    public Date getPickup() {
        return pickup;
    }

    public Date getDropoff() {
        return dropoff;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public long getTripTimeInSecs() {
        return tripTimeInSecs;
    }

    public double getPickupLongitude() {
        return pickupLongitude;
    }

    public double getPickupLatitude() {
        return pickupLatitude;
    }

    public double getDropoffLongitude() {
        return dropoffLongitude;
    }

    public double getDropoffLatitude() {
        return dropoffLatitude;
    }

    @Override
    public String toString() {
        return "DEBSFullRecord{" +
                "medallion='" + medallion + '\'' +
                ", hackLicense='" + hackLicense + '\'' +
                ", vendorID='" + vendorID + '\'' +
                ", storeAndFwdFlag='" + storeAndFwdFlag + '\'' +
                ", pickup=" + pickup +
                ", dropoff=" + dropoff +
                ", passengerCount=" + passengerCount +
                ", tripTimeInSecs=" + tripTimeInSecs +
                ", pickupLongitude=" + pickupLongitude +
                ", pickupLatitude=" + pickupLatitude +
                ", dropoffLongitude=" + dropoffLongitude +
                ", dropoffLatitude=" + dropoffLatitude +
                '}';
    }
}
