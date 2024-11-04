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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

public class weekly_balance { // based on example

  // for the data: "<date> tab <sum1>,<sum2>", transfer the date to weekday(Monday-Sunday), 
  // and calculate the average sum1 and sum2 in each weekday
  
  public static class flowMapper extends
  Mapper<Object, Text, Text, Text> {

      private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
      private static final String[] weekdays = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
      @Override
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String dateStr = tokens[0];
        String []sums = tokens[1].split(",");
        String purchase= sums[0];
        String redeem = sums[1];

        try {
          Date date = dateFormat.parse(dateStr);
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(date);
          int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
          String weekday = weekdays[dayOfWeek - 1]; // Calendar.DAY_OF_WEEK returns 1 for Sunday, 2 for Monday, etc.

          context.write(new Text(weekday), new Text(purchase + "," + redeem));
        } catch (ParseException e) {
          e.printStackTrace();
      }
    }
  }
    
    

  public static class flowReducer extends
  Reducer<Text, Text, Text, Text> {
    @Override    
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException{
      long purchase_sum = 0;
      long redeem_sum = 0;
      long cnt=0;
      for (Text value : values) {
        String[] tokens = value.toString().split(",");
        if(tokens.length >1){
          try{
            purchase_sum += Long.parseLong(tokens[0]);
            redeem_sum += Long.parseLong(tokens[1]);
            cnt++;
          }catch(NumberFormatException e){
            System.out.println("Error in parsing");
          }
        }
      }
      long purchase_avg=purchase_sum/cnt;
      long redeem_avg=redeem_sum/cnt;
      context.write(key, new Text(purchase_avg + "," + redeem_avg));
    }    
}

    

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "weekly balance");
    job.setJarByClass(weekly_balance.class);
    job.setMapperClass(flowMapper.class);
    job.setReducerClass(flowReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}