import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text,CompositeValueClass,Text,CompositeValueClass> {

    @Override
    protected void reduce(Text key, Iterable<CompositeValueClass> values, Context context) throws IOException, InterruptedException {
        CompositeValueClass result = new CompositeValueClass(); //store final % value
        double totalCarrierDelay = 0.0;
        double totalWeatherDelay = 0.0;
        double totalNasDelay = 0.0;
        double totalSecurityDelay = 0.0;
        double totalLateAircraft = 0.0;
        double totalAirlineDelay = 0.0;
        for(CompositeValueClass cv:values){
            totalCarrierDelay += cv.getCarrierDelay();
            totalWeatherDelay += cv.getWeatherDelay();
            totalNasDelay += cv.getNasDelay();
            totalSecurityDelay += cv.getSecurityDelay();
            totalLateAircraft += cv.getLateAircraftDelay();
            totalAirlineDelay += cv.getTotalAirlineDelay();
        }

        result.setCarrierDelay((totalCarrierDelay/totalAirlineDelay)*100);
        result.setWeatherDelay((totalWeatherDelay/totalAirlineDelay)*100);
        result.setNasDelay((totalNasDelay/totalAirlineDelay)*100);
        result.setSecurityDelay((totalSecurityDelay/totalAirlineDelay)*100);
        result.setLateAircraftDelay((totalLateAircraft/totalAirlineDelay)*100);

        context.write(key,result);
    }
}
