package util;/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.text.SimpleDateFormat;

/**
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class DEBSRecordParser {
    private static final SimpleDateFormat DATE_TIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private String medallion;
    private long tripTimeSecs;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        tripTimeSecs = DATE_TIME_FORMATTER.parse(splitted[3]).getTime()
                - DATE_TIME_FORMATTER.parse(splitted[2]).getTime();
    }

    public String getMedallion() {
        return medallion;
    }

    public long getTripTimeSecs() {
        return tripTimeSecs;
    }

}
