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
    private long tripTimeSecs;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        tripTimeSecs = Integer.parseInt(splitted[8]);
    }

    public String getMedallion() {
        return medallion;
    }

    public long getTripTimeSecs() {
        return tripTimeSecs;
    }

}
