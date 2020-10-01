import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,CompositeKeyClass, DoubleWritable> {
    //Busiest airport in USA? (Secondary sort by year)
    DoubleWritable arrivingFlights = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tokens = line.split(",",-1);

        String airportName = null;
        String observationDate = null;
        double numOfFlights = 0.0;
        try{
            observationDate = tokens[0];
            airportName = tokens[5];
            numOfFlights = Double.parseDouble(tokens[7]);
            arrivingFlights.set(numOfFlights);
            System.out.println(observationDate);
            System.out.println(airportName);
            System.out.println(numOfFlights);
            CompositeKeyClass obj = new CompositeKeyClass(airportName,observationDate);
            context.write(obj,arrivingFlights);

        }catch (Exception e){}


    }
}
