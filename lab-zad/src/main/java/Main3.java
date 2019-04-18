import mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CellWritable;

import java.io.IOException;

public class Main3 {
    private static final Path INTERMEDIATE_PATH = new Path("/user/rovkp/dariansaric/intermediate");
    private static final Path INTERMEDIATE_PATH1 = new Path("/user/rovkp/dariansaric/intermediate1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: ParititionJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main1.class);
        job.setJobName("FilteringJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, INTERMEDIATE_PATH);

        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0);
//        job.setCombinerClass(TripTimeReducer.class);
//        job.setReducerClass(TripTimeReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        long start = System.currentTimeMillis();

        System.out.println("Započinje posao '" + job.getJobName() + "'");
        if (!job.waitForCompletion(true)) {
            System.err.println("Posao '" + job.getJobName() + "' nije obavljen, izlazim...");
            System.exit(-1);
        }
        long interimTime = System.currentTimeMillis();
        System.out.println("Posao '" + job.getJobName() + "' je upjesno obavljen u " + (interimTime - start) + "ms");

        Job job1 = Job.getInstance();
        job1.setJarByClass(Main2.class);
        job1.setJobName("CellJob");

        FileInputFormat.addInputPath(job1, INTERMEDIATE_PATH);
        FileOutputFormat.setOutputPath(job1, INTERMEDIATE_PATH1);

        job1.setMapperClass(CellMapper.class);
        job1.setReducerClass(CellReducer.class);
//        job.setPartitionerClass(CellPartitioner.class);
//        job.setNumReduceTasks(150 * 150);

//        job.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(CellWritable.class);
        job1.setOutputValueClass(Text.class);

        long interimStart = System.currentTimeMillis();
        System.out.println("Započinje posao '" + job1.getJobName() + "'");

        if (!job1.waitForCompletion(true)) {
            System.err.println("Posao '" + job1.getJobName() + "' nije uspjesno obavljen -> posao nije obavljen");
            System.exit(-1);
        }

        long timeEnd = System.currentTimeMillis();
        System.out.println("Posao '" + job1.getJobName() + "' je upjesno obavljen u " + (timeEnd - interimStart) + "ms");

        Job job2 = Job.getInstance();
        job2.setJobName("Final analyzer");

        FileInputFormat.addInputPath(job2, INTERMEDIATE_PATH1);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.out.println("Započinje posao '" + job2.getJobName() + "'");
        System.out.println(String.format("Posao '" + job2.getJobName() + "'%sje obavljen...", job2.waitForCompletion(true) ? " " : " ni"));
        long timeEnd2 = System.currentTimeMillis();
        System.out.println("Vrijeme izvrsavanja: " + (timeEnd2 - timeEnd) + " ms");

        System.out.println("Posao je uspjesno obavljen u " + (timeEnd2 - start) + "ms");

        FileSystem.get(new Configuration()).delete(INTERMEDIATE_PATH, true);
        FileSystem.get(new Configuration()).delete(INTERMEDIATE_PATH1, true);
    }
}
