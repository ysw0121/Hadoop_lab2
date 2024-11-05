import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;

import javax.naming.Context;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class other_factors {
    

    public static class otherMapper 
        extends Mapper<Object, Text, Text, IntWritable> {
            private boolean isFirstLine = true;
       public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            String line = value.toString().trim();
            if(isFirstLine){
                isFirstLine = false;
                return;
            }
            String[] tokens = line.split(",\\s*");
            String date = tokens[0];
            Double rate_0= Double.parseDouble(tokens[1]);
            Double rate_y= Double.parseDouble(tokens[8]);
            if(rate_0>=rate_y){ // day rate > yearly rate(avg)
                context.write(new Text(date), new IntWritable(1));
            }
            else{
                context.write(new Text(date), new IntWritable(0));
            }
       }
    }
    public static class otherReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
            private int count = 0;
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                
                for (IntWritable val : values) {
                    context.write(key, val);
                    count += val.get();
                }
                 
                
            }
            public void cleanup(Context context) throws IOException, InterruptedException {
                String res="Shibor daily rate is higher than same-day yearly rate(avg) for "+count+" days"+"\n";
                context.write(new Text(res), null);
                
            }
  
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "others");
        job.setJarByClass(other_factors.class);
        job.setMapperClass(otherMapper.class);
        job.setReducerClass(otherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
