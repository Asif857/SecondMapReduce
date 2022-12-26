package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Main {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String currKey = itr.nextToken();
                String val = itr.nextToken();
                word.set("0" + "," + currKey);
                Text wordPlusOne = new Text("1" + "," + currKey);
                String[] values = val.split(",");
                context.write(new Text("0N" + values[0]),wordPlusOne);
                context.write(new Text("1N" + values[1]),wordPlusOne);
                context.write(new Text("0T" + values[0]),new Text(values[1] + "," + currKey));
                context.write(new Text("1T" + values[1]),new Text(values[0] + "," + currKey));
                context.write(new Text("r" + values[2]),new Text(word));
            }
        }
    }
    public static class ParametersReducer
            extends Reducer<Text,Text,Text,Text> {
        float N0 = 0;
        float N1 = 0;
        float T0 = 0;
        float T1 = 0;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.toString().charAt(0) == 'r'){
                float N = Float.parseFloat(context.getConfiguration().get("N"));
                for (Text value: values){
                    float pr = (T1 + T0)/(N*(N1 + N0));
                    String newkey = value.toString().replaceAll(",", " ");
                    context.write(new Text(newkey),new Text(String.valueOf(pr)));
                }
                 N0 = 0;
                 N1 = 0;
                 T0 = 0;
                 T1 = 0;
            }
            else {
                switch (key.toString().substring(0,2)){
                    case "0N":
                }

            }
            }
        }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EMR2");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(ParametersReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}