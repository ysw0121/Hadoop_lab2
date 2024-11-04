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

public class daily_balance { // based on example

  public static class flowMapper extends
      Mapper<LongWritable, Text, Text, Text > {
        private boolean isFirstLine = true;
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
              if (isFirstLine) {
                isFirstLine = false;
                return;
              }
              String[] tokens = value.toString().split(",");
              String date = tokens[1];
              String purchase_amount=tokens[4];
              if(purchase_amount.isEmpty()){
                purchase_amount = "0";
              } 
              String redeem_amount=tokens[8];
              if(redeem_amount.isEmpty()){
                redeem_amount = "0";
              } 
              context.write(new Text(date), new Text(purchase_amount + "," + redeem_amount));
            }

    
    }

  public static class flowReducer extends
      Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
              long purchase_sum = 0;
              long redeem_sum = 0;
              for (Text value : values) {
                String[] tokens = value.toString().split(",");
                if(tokens.length >1){
                  try{
                    purchase_sum += Long.parseLong(tokens[0]);
                    redeem_sum += Long.parseLong(tokens[1]);
                  }catch(NumberFormatException e){
                    System.out.println("Error in parsing");
                  }
                }
              }
              context.write(key, new Text(purchase_sum + "," + redeem_sum));
            }
    
    }

    

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "daily balance");
    job.setJarByClass(daily_balance.class);
    job.setMapperClass(flowMapper.class);
    job.setReducerClass(flowReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}