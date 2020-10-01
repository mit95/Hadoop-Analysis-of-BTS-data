import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        // First MapReduce
        JobControl jobControl = new JobControl("jobChain");
        Configuration cnf1 = new Configuration();

        Job job1 = Job.getInstance(cnf1);
        job1.setJarByClass(DriverClass.class);
        job1.setJobName("MR1");

        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out,"out1"));//store partial results

        job1.setMapperClass(MapperClass.class);
        job1.setReducerClass(ReducerClass.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        ControlledJob controlledJob1 = new ControlledJob(cnf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        // Second MapReduce

        Configuration cnf2 = new Configuration();

        Job job2 = Job.getInstance(cnf2);
        job2.setJarByClass(DriverClass.class);
        job2.setJobName("MR2");

        job2.setMapperClass(MapperClass2.class);
        job2.setReducerClass(ReducerClass2.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        ControlledJob controlledJob2 = new ControlledJob(cnf2);
        controlledJob2.setJob(job2);

        FileInputFormat.setInputPaths(job2, new Path(out,"out1"));//get partial result from mr1
        FileOutputFormat.setOutputPath(job2, new Path(out,"out2"));

        //FileSystem fs = FileSystem.get(cnf1);
        //fs.delete(new Path(args[1]), true);

		// job2 is dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
		// add the job to the job control
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        //System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
