import javafx.scene.text.Text;
import mapreduce.LocationPassengerPartitioner;
import mapreduce.PartitionMapper;
import mapreduce.PartitionReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.RecordTuple;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: ParititionJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Partitioning job");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        job.setPartitionerClass(LocationPassengerPartitioner.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RecordTuple.class);

        long start = System.currentTimeMillis();

        System.out.println("Započinje posao...");
//        job.submit();
        System.out.println(String.format("Posao%sje obavljen...", job.waitForCompletion(true) ? " " : " ni"));
        System.out.println("Vrijeme izvrsavanja: " + (System.currentTimeMillis() - start) + " ms");
    }
}
