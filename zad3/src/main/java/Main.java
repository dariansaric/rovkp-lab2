import mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.SimpleRecordTuple;
import util.TripTimeTuple;

import java.io.IOException;

public class Main {
    private static final Path INTERMEDIATE_PATH = new Path("/intermediate");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: ParititionJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Partitioning job");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, INTERMEDIATE_PATH);

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        job.setPartitionerClass(LocationPassengerPartitioner.class);
        job.setNumReduceTasks(6);
//        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SimpleRecordTuple.class);

        long start = System.currentTimeMillis();

        System.out.println("Započinje posao '" + job.getJobName() + "'");
        if (!job.waitForCompletion(true)) {
            System.err.println("Posao '" + job.getJobName() + "' nije obavljen, izlazim...");
            System.exit(-1);
        }
        long interimTime = System.currentTimeMillis();
        System.out.println("Posao '" + job.getJobName() + "' je upjesno obavljen u " + (interimTime - start) + "ms");

        Job job1 = Job.getInstance();
        job1.setJarByClass(Main.class);
        job1.setJobName("TripTime analytics");

        FileInputFormat.addInputPath(job1, INTERMEDIATE_PATH);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setMapperClass(TripTimeMapper.class);
        job1.setCombinerClass(TripTimeReducer.class);
        job1.setReducerClass(TripTimeReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TripTimeTuple.class);

        long interimStart = System.currentTimeMillis();

        System.out.println("Započinje posao '" + job1.getJobName() + "'");

        if (!job1.waitForCompletion(true)) {
            System.err.println("Posao '" + job1.getJobName() + "' nije uspjesno obavljen -> posao nije obavljen");
            System.exit(-1);
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("Posao '" + job1.getJobName() + "' je upjesno obavljen u " + (timeEnd - interimStart) + "ms");

        System.out.println("Posao je uspjesno obavljen u " + (timeEnd - start) + "ms");

//        FileSystem.get(new Configuration()).delete(INTERMEDIATE_PATH, true);
    }
}
