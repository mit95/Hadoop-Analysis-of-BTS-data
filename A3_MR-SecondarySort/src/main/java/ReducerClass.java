import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<CompositeKeyClass, DoubleWritable, Text,DoubleWritable> {

    Text resultKey = new Text();
    DoubleWritable flightCount = new DoubleWritable();
    @Override
    protected void reduce(CompositeKeyClass key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double arrivingFlightsSum = 0.0;

        for(DoubleWritable val:values){
            arrivingFlightsSum += val.get();
        }

        resultKey.set("Year: "+key.getObservationDate()+" airportName: "+key.getAirportName());
        flightCount.set(arrivingFlightsSum);
        context.write(resultKey,flightCount);
    }
}
