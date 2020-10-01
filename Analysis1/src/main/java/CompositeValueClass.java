import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValueClass implements WritableComparable<CompositeValueClass> {

    double carrierDelay;
    double weatherDelay;
    double nasDelay;
    double securityDelay;
    double lateAircraftDelay;
    double totalAirlineDelay;

    public CompositeValueClass(double carrierDelay, double weatherDelay, double nasDelay, double securityDelay, double lateAircraftDelay, double totalAirlineDelay) {
        this.carrierDelay = carrierDelay;
        this.weatherDelay = weatherDelay;
        this.nasDelay = nasDelay;
        this.securityDelay = securityDelay;
        this.lateAircraftDelay = lateAircraftDelay;
        this.totalAirlineDelay = totalAirlineDelay;
    }

    public CompositeValueClass() { }

    public double getTotalAirlineDelay() {
        return totalAirlineDelay;
    }

    public void setTotalAirlineDelay(double totalAirlineDelay) {
        this.totalAirlineDelay = totalAirlineDelay;
    }

    public double getCarrierDelay() {
        return carrierDelay;
    }

    public void setCarrierDelay(double carrierDelay) {
        this.carrierDelay = carrierDelay;
    }

    public double getWeatherDelay() {
        return weatherDelay;
    }

    public void setWeatherDelay(double weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public double getNasDelay() {
        return nasDelay;
    }

    public void setNasDelay(double nasDelay) {
        this.nasDelay = nasDelay;
    }

    public double getSecurityDelay() {
        return securityDelay;
    }

    public void setSecurityDelay(double securityDelay) {
        this.securityDelay = securityDelay;
    }

    public double getLateAircraftDelay() {
        return lateAircraftDelay;
    }

    public void setLateAircraftDelay(double lateAircraftDelay) {
        this.lateAircraftDelay = lateAircraftDelay;
    }

    @Override
    public String toString() {
        return "-> Carrier: "+carrierDelay+"% "+"Weather: "+weatherDelay+"% "+"Nas: "+nasDelay+"% "+"Security: "+securityDelay+"% "+"Late Aircraft: "+lateAircraftDelay+"%";
    }

    public int compareTo(CompositeValueClass compositeValueClass) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(carrierDelay);
        out.writeDouble(weatherDelay);
        out.writeDouble(nasDelay);
        out.writeDouble(securityDelay);
        out.writeDouble(lateAircraftDelay);
        out.writeDouble(totalAirlineDelay);
    }

    public void readFields(DataInput in) throws IOException {
        carrierDelay =in.readDouble();
        weatherDelay =in.readDouble();
        nasDelay=in.readDouble();
        securityDelay=in.readDouble();
        lateAircraftDelay=in.readDouble();
        totalAirlineDelay=in.readDouble();
    }
}
