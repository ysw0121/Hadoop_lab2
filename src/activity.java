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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class activity { // based on example

  public static class actMapper extends
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
            String user_id = tokens[0];
            String dir_purchase=tokens[5];
            if(dir_purchase.isEmpty()){
              dir_purchase = "0";
            } 
            String redeem_amount=tokens[8];
            if(redeem_amount.isEmpty()){
              redeem_amount = "0";
            } 
            context.write(new Text(user_id), new Text(dir_purchase + "," + redeem_amount));
          }    
  }


  public static class actReducer extends
      Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> userMap = new HashMap<String, Integer>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
              int cnt=0;
              for(Text value : values){
                String[] tokens = value.toString().split(",");
                long purchase = Long.parseLong(tokens[0]);
                long redeem = Long.parseLong(tokens[1]);
                if(purchase > 0 || redeem > 0){
                  cnt++;
                }
              }
              userMap.put(key.toString(), cnt);
            }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
          List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(userMap.entrySet());
          sortedList.sort((o1, o2) -> o2.getValue()-o1.getValue());
          for(Map.Entry<String, Integer> entry : sortedList){
            context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
          }
        }

      }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "user activity");
    job.setJarByClass(activity.class);
    job.setMapperClass(actMapper.class);
    job.setReducerClass(actReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
