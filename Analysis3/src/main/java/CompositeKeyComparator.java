import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator(){
        super(CompositeKeyClass.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyClass ckw1 = (CompositeKeyClass) a;
        CompositeKeyClass ckw2 = (CompositeKeyClass) b;

        DateFormat f = new SimpleDateFormat("yyyy");
        int result = 0;
        try {
            result = -1* f.parse(ckw1.getObservationDate()).compareTo(f.parse(ckw2.getObservationDate()));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return result;
    }
}
