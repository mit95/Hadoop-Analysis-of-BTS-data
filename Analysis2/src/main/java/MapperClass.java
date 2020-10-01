import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    //Top 10 most punctual flights (MR chaining)
    DoubleWritable  numOfDelayedFlights = new DoubleWritable();
    Text carrierName = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tokens = line.split(",",-1);
        double delayedFlights=0.0;
        //Emit the carrier name and number of flights that were delayed for every observation
        try{
            if(tokens[8]!=null && !tokens[8].isEmpty()){
                delayedFlights = Double.parseDouble(tokens[8]);
                if(delayedFlights > 0) {
                    numOfDelayedFlights.set(delayedFlights);
                    carrierName.set(tokens[3]);
                    context.write(carrierName, numOfDelayedFlights);
                }
            }
        }
        catch(Exception e){
           e.printStackTrace();
        }

    }
}
