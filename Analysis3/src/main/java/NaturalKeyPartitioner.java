import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyClass, DoubleWritable> {

    @Override
    public int getPartition(CompositeKeyClass key, DoubleWritable value, int noOfPartitions) {
        String temp = key.getObservationDate();
        String tempi = key.getAirportName();
        return key.getAirportName().hashCode() % noOfPartitions;
    }
}
