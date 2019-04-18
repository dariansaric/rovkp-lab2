import mapreduce.CellMapper;
import mapreduce.CellReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CellWritable;

import java.io.IOException;

public class Main2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: TripTimeJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main2.class);
        job.setJobName("CellJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CellMapper.class);
        job.setReducerClass(CellReducer.class);
//        job.setPartitionerClass(CellPartitioner.class);
//        job.setNumReduceTasks(150 * 150);

//        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(CellWritable.class);
        job.setOutputValueClass(Text.class);


        System.out.println("Započinje posao '" + job.getJobName() + "'");
        System.out.println(String.format("Posao '" + job.getJobName() + "'%sje obavljen...", job.waitForCompletion(true) ? " " : " ni"));
        System.out.println("Vrijeme izvrsavanja: " + (job.getFinishTime() - job.getStartTime()) + " ms");
    }
}
