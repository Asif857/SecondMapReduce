package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.checkerframework.checker.units.qual.C;

import java.io.DataInput;
import java.io.DataOutput;
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
                word.set(currKey);
                String[] values = val.split(",");
                context.write(new Text(values[0] + "0N"),word);
                context.write(new Text(values[1] + "1N"),word);
                context.write(new Text(values[0] + "0T"),new Text(values[1] + "," + currKey));
                context.write(new Text(values[1] + "1T"),new Text(values[0] + "," + currKey));
                context.write(new Text(values[2] + "1U"),new Text(word));
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
            System.out.println(key);
                switch (key.toString().substring(key.getLength() - 2)){
                    case "0N":
                        for(Text value : values)
                            N0++;
                        break;
                    case "1N":
                        for(Text value : values)
                            N1++;
                        break;
                    case "0T":
                        for(Text value : values)
                            T0 += Float.parseFloat(value.toString().split(",")[0]);
                        break;
                    case "1T":
                        for(Text value : values)
                            T1 += Float.parseFloat(value.toString().split(",")[0]);
                        break;
                    case "1U":
                        float N = Float.parseFloat(context.getConfiguration().get("N"));
                        float pr = -1;
                        System.out.println("r: "+key.toString().substring(0,key.getLength() - 2)+", N1: "+N1+", N0: "+N0+", +T0: "+T0+", +T1: "+T1);
                        if (N1 != 0 || N0 != 0) {
                            pr = (T1 + T0) / (N * (N1 + N0));
                        }
                        for (Text value: values){
                            String newValue = value.toString().replaceAll(",", " ");
                            context.write(new Text(newValue),new Text(String.valueOf(pr)));
                        }
                        N0 = 0;
                        N1 = 0;
                        T0 = 0;
                        T1 = 0;
                        break;
                }
            }
            }
    public static class CustomPartitioner extends Partitioner<Text,Text> {
        public int getPartition(Text key, Text value, int numOfReducers) {
            int r = Integer.parseInt(key.toString().substring(2));
            return (r % numOfReducers);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("N","58");
        Job job = Job.getInstance(conf, "EMR2");
        int numReducers = job.getNumReduceTasks();
        System.out.println(conf.get("N"));
        job.setPartitionerClass(CustomPartitioner.class);
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