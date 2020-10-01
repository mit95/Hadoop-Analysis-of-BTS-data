import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyClass implements WritableComparable<CompositeKeyClass> {

    String airportName;
    String observationDate;

    public CompositeKeyClass(String airportName, String observationDate) {
        this.airportName = airportName;
        this.observationDate = observationDate;
    }

    public CompositeKeyClass() {
    }

    public String getAirportName() {
        return airportName;
    }

    public void setAirportName(String airportName) {
        this.airportName = airportName;
    }

    public String getObservationDate() {
        return observationDate;
    }

    public void setObservationDate(String observationDate) {
        this.observationDate = observationDate;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public int compareTo(CompositeKeyClass o) {
        int result = this.observationDate.compareTo(o.getObservationDate());
        return (result < 0? -1:(result==0?0:1));
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(airportName);
        out.writeUTF(observationDate);
    }

    public void readFields(DataInput in) throws IOException {
        airportName = in.readUTF();
        observationDate = in.readUTF();
    }
}
