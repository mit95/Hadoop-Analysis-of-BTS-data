import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//Analysis 1:What is the main cause of delay for every airline?
public class MapperClass extends Mapper<LongWritable, Text,Text,CompositeValueClass> {

    Text airlinerName = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split(",",-1);
        double totalDelay = 0.0;
        //Emit Airline name and a composite object consisting of every type of delay
        try{
            airlinerName.set(token[3]);
            CompositeValueClass cv = new CompositeValueClass();
            totalDelay = Double.parseDouble(token[16]);
            if(token[16] !=null && !token[16].isEmpty() && totalDelay > 0) {
                cv.setTotalAirlineDelay(totalDelay);// to calculate final %
                //set this only if there is a delay
                if(token[17] !=null && !token[17].isEmpty()) {
                    cv.setCarrierDelay(Double.parseDouble(token[17]));
                }
                if(token[18] !=null && !token[18].isEmpty()) {
                    cv.setWeatherDelay(Double.parseDouble(token[18]));
                }
                if(token[19] !=null && !token[19].isEmpty()) {
                    cv.setNasDelay(Double.parseDouble(token[19]));
                }
                if(token[20] !=null && !token[20].isEmpty()) {
                    cv.setSecurityDelay(Double.parseDouble(token[20]));
                }
                if(token[21] !=null && !token[21].isEmpty()) {
                    cv.setLateAircraftDelay(Double.parseDouble(token[21]));
                }
                context.write(airlinerName,cv);
            }
        }
        catch(Exception e){
            //e.printStackTrace();
        }

    }
}
