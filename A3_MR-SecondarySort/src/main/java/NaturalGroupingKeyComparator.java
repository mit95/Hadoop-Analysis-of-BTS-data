import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalGroupingKeyComparator extends WritableComparator {

    public NaturalGroupingKeyComparator(){
        super(CompositeKeyClass.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyClass ckw1 = (CompositeKeyClass) a;
        CompositeKeyClass ckw2 = (CompositeKeyClass) b;
        String temp = ckw1.getObservationDate();
        String tempi = ckw2.getAirportName();

        int result = ckw1.getAirportName().compareTo(ckw2.getAirportName());

        return result;
    }
}
