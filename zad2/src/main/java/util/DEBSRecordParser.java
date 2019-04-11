package util;/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class DEBSRecordParser {
    private String medallion;
    private double[] startLocation = new double[2];
    private double[] endLocaltion = new double[2];
    private int noOfPassengers;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        startLocation[0] = Double.parseDouble(splitted[11]);
        startLocation[1] = Double.parseDouble(splitted[12]);
        endLocaltion[0] = Double.parseDouble(splitted[13]);
        endLocaltion[1] = Double.parseDouble(splitted[14]);
        noOfPassengers = Integer.parseInt(splitted[8]);
    }

    public int getNoOfPassengers() {
        return noOfPassengers;
    }

    public double[] getStartLocation() {
        return startLocation;
    }

    public double[] getEndLocaltion() {
        return endLocaltion;
    }

    public String getMedallion() {
        return medallion;
    }
}
