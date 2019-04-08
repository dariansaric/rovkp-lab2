import madreduce.TripTimeMapper;
import madreduce.TripTimeReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //todo: isprobati :P
        if (args.length != 2) {
            System.err.println("Usage: TripTimeJob <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("TripTime analytics - bla");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("test"));

        job.setMapperClass(TripTimeMapper.class);
//        job.setCombinerClass(TripTimeReducer.class);
        job.setReducerClass(TripTimeReducer.class);

        long start = System.currentTimeMillis();

        System.out.println(String.format("Posao%sje obavljen...", job.waitForCompletion(true) ? " " : " ni"));
        System.out.println("Vrijeme izvrsavanja: " + (System.currentTimeMillis() - start));
    }
}
