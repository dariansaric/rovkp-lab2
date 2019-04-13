package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class RecordTuple implements Writable {
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private DEBSFullRecord record;

    public RecordTuple(DEBSFullRecord record) {
        this.record = record;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(record.getMedallion());
        dataOutput.writeChars(record.getHackLicense());
        dataOutput.writeChars(record.getVendorID());
        dataOutput.writeInt(record.getRateCode());
        dataOutput.writeChars(record.getStoreAndFwdFlag());
        dataOutput.writeChars(DATE_FORMATTER.format(record.getPickup()));
        dataOutput.writeChars(DATE_FORMATTER.format(record.getDropoff()));
        dataOutput.writeInt(record.getPassengerCount());
        dataOutput.writeLong(record.getTripTimeInSecs());
        dataOutput.writeDouble(record.getTripDistance());
        dataOutput.writeDouble(record.getPickupLongitude());
        dataOutput.writeDouble(record.getPickupLatitude());
        dataOutput.writeDouble(record.getDropoffLongitude());
        dataOutput.writeDouble(record.getDropoffLatitude());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        try {
            record = new DEBSFullRecord(
                    dataInput.readUTF()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DEBSFullRecord getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return record.toString();
    }
}

