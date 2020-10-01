import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass2 extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
    int counter = 0;
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //write the first ten records which correspond to carriers with least amount of delayed flights
        for(Text val:values){
            counter ++;
            if(counter <=10){
                context.write(key,val);
            }
        }
    }
}
