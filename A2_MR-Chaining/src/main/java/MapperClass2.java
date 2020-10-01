import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass2 extends Mapper<LongWritable, Text,DoubleWritable,Text> {

    DoubleWritable delayedFlights = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        //Emit the total delayed flights and carrier name
        //emit delayed flights count as key so that it will be sorted by the framework
        Text carrierName = new Text(tokens[0]);
        double delayedFlightCount = Double.parseDouble(tokens[1]);

        delayedFlights.set(delayedFlightCount);

        context.write(delayedFlights,carrierName);
    }
}
