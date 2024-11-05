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

public class other_factors_failed { // based on example

  public static class otherMapper extends
      Mapper<Object, Text, Text, Text> {

        private Map<String, String> user_pro = new HashMap<String, String>();
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
          String profile = context.getConfiguration().get("profile");
          Path path = new Path(profile);
          FileSystem fs = FileSystem.get(new Configuration());
          FSDataInputStream in = fs.open(path);
          if (in == null) {
            throw new IOException("File not found: " + profile);
          }
          BufferedReader br = new BufferedReader(new InputStreamReader(in));

          String line = br.readLine();
          Boolean flag = false;
          String user = "";
          while (line != null) {
            
            if(flag) {  // skip the first line
              user_pro.put(user, line); 
            }
            flag=true;
            line = br.readLine();
            if(line==null){
              break;
            }
            if(line.length()!=0) {
              String []tokens = line.split(",\\s*");
              user= tokens[0];
              line=tokens[1]+","+tokens[2]+","+tokens[3];
            }
          }
          br.close();
        }
       
        // @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String info = user_pro.get(key.toString());
          if(info!=null){
            String[] tokens=info.split(",\\s*");
            context.write(new Text(key.toString()), new Text(tokens[0]+","+tokens[1]+","+tokens[2]+","+value.toString()));
          // key: user_id, value: sex, city, sign, act_times
          }
          
        }
    

    
    }

  public static class otherReducer extends
      Reducer<Text, Text, Text, Text> {
        private Map<String, String> user_pro = new HashMap<String, String>();
        private Map<String, Integer> sex_less = new HashMap<String, Integer>();
        private Map<String, Integer> sex_more = new HashMap<String, Integer>();
        private Map<String, Integer> city_less = new HashMap<String, Integer>();
        private Map<String, Integer> city_more = new HashMap<String, Integer>();
        private Map<String, Integer> sign_less = new HashMap<String, Integer>();
        private Map<String, Integer> sign_more = new HashMap<String, Integer>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
              long act_times=0;
              long all_cnt=0;
              // int test_cn=0;
              for (Text value : values) {
                String []test = value.toString().split("\t");
                System.out.println(value.toString());
                String[] tokens = test[0].split(",");
                act_times+=Long.parseLong(test[1]);
                all_cnt++;
              }
              act_times=act_times/all_cnt;
              
              for(Text value: values) {
                String []test = value.toString().split("\t");
                System.out.println(value.toString());
                String[] tokens = test[0].split(",");
                // String user = tokens[0];
                String sex = tokens[0];
                String city = tokens[1];
                String sign = tokens[2];
                Long user_act_times = Long.parseLong(test[1]);
                if(user_act_times<act_times) {
                  if(sex_less.containsKey(sex)) {
                    sex_less.put(sex, sex_less.get(sex)+1);
                  } else {
                    sex_less.put(sex, 1);
                  }
                  if(city_less.containsKey(city)) {
                    city_less.put(city, city_less.get(city)+1);
                  } else {
                    city_less.put(city, 1);
                  }
                  if(sign_less.containsKey(sign)) {
                    sign_less.put(sign, sign_less.get(sign)+1);
                  } else {
                    sign_less.put(sign, 1);
                  }
                }
                if(user_act_times>=act_times) {
                  if(sex_more.containsKey(sex)) {
                    sex_more.put(sex, sex_more.get(sex)+1);
                  } else {
                    sex_more.put(sex, 1);
                  }
                  if(city_more.containsKey(city)) {
                    city_more.put(city, city_more.get(city)+1);
                  } else {
                    city_more.put(city, 1);
                  }
                  if(sign_more.containsKey(sign)) {
                    sign_more.put(sign, sign_more.get(sign)+1);
                  } else {
                    sign_more.put(sign, 1);
                  }
                }
              }
            }
          public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> sex_less_list = new ArrayList<>(sex_less.entrySet());
            List<Map.Entry<String, Integer>> city_less_list = new ArrayList<Map.Entry<String, Integer>>(city_less.entrySet());
            List<Map.Entry<String, Integer>> sign_less_list = new ArrayList<Map.Entry<String, Integer>>(sign_less.entrySet());
            List<Map.Entry<String, Integer>> sex_more_list = new ArrayList<Map.Entry<String, Integer>>(sex_more.entrySet());
            List<Map.Entry<String, Integer>> city_more_list = new ArrayList<Map.Entry<String, Integer>>(city_more.entrySet());
            List<Map.Entry<String, Integer>> sign_more_list = new ArrayList<Map.Entry<String, Integer>>(sign_more.entrySet());
            context.write(new Text("result of sex less than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: sex_less_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }
              context.write(new Text("result of cities less than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: city_less_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }
              context.write(new Text("result of constellation less than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: sign_less_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }
              context.write(new Text("result of sex more than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: sex_more_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }
              context.write(new Text("result of cities more than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: city_more_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }
              context.write(new Text("result of constellation more than avg act_times"),null);
              for(Map.Entry<String, Integer> entry: sign_more_list) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
              }

          }
            
    }

    

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("profile", args[2]);
    Job job = new Job(conf, "other_factors");
    job.setJarByClass(other_factors_failed.class);
    job.setMapperClass(otherMapper.class);
    job.setReducerClass(otherReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}