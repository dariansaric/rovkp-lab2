import mapreduce.FilterMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: TripTimeJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main1.class);
        job.setJobName("FilteringJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FilterMapper.class);
//        job.setCombinerClass(TripTimeReducer.class);
//        job.setReducerClass(TripTimeReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

//        long start = System.currentTimeMillis();

        System.out.println("Započinje posao '" + job.getJobName() + "'");
        System.out.println(String.format("Posao '" + job.getJobName() + "'%sje obavljen...", job.waitForCompletion(true) ? " " : " ni"));
        System.out.println("Vrijeme izvrsavanja: " + (job.getFinishTime() - job.getStartTime()) + " ms");
    }
}
