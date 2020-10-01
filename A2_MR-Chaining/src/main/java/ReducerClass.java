import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

    DoubleWritable numOfDelayedFlights = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalDelayedFlights = 0.0;
        //calculate the total number of delayed flights / carrier
        for(DoubleWritable value:values){
            totalDelayedFlights += value.get();
        }

        numOfDelayedFlights.set(totalDelayedFlights);

        context.write(key,numOfDelayedFlights);
    }
}
