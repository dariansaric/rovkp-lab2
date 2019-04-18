package util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public class CellWritable implements WritableComparable<CellWritable> {
    private static final double BEGIN_LON = -74.913585;
    private static final double GRID_LENGTH = 0.011972;
    private static final double BEGIN_LAT = 41.474937;
    private static final double GRID_WIDTH = 0.00893112;
    public static Comparator<CellWritable> COORDINATE_COMPARATOR =
            (cw1, cw2) -> cw1.x == cw2.x ? 0 : Integer.compare(cw1.y, cw2.y);
    public static Comparator<CellWritable> HASH_COMPARATOR =
            Comparator.comparingInt(Object::hashCode);
    private int x;
    private int y;

    public CellWritable() {
    }

    public CellWritable(double dropoffLongitude, double dropoffLatitude) {
        this.x = (int) ((dropoffLongitude - BEGIN_LON) / GRID_LENGTH) + 1;
        this.y = (int) ((BEGIN_LAT - dropoffLatitude) / GRID_WIDTH) + 1;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setX(dataInput.readInt());
        setY(dataInput.readInt());
    }

    @Override
    public String toString() {
        return x + "," + y;
    }


    @Override
    public int compareTo(CellWritable o) {
        return HASH_COMPARATOR.compare(this, o) == 0 ? 0 : COORDINATE_COMPARATOR.compare(this, o);
    }
}
