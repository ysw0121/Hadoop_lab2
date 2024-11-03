import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class activity { // based on example

  public static class actMapper extends
      Mapper<> {

    

    
    }

  public static class actReducer extends
      Reducer<> {
    

    
    }

    

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("stopwordlist", args[2]);
    Job job = new Job(conf, "daily_balance");
    job.setJarByClass();
    job.setMapperClass();
    job.setReducerClass();
    job.setOutputKeyClass();
    job.setOutputValueClass();
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}