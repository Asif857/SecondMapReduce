package org.example;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
                word.set(currKey);
                if (currKey.equals("*")) {
                    context.write(word, new Text(val));
                } else {
                    String[] values = val.split(",");
                    context.write(new Text("0N" + values[0]),word);
                    context.write(new Text("1N" + values[1]),word);
                    context.write(new Text("0T" + values[0]),new Text(values[1] + "," + currKey));
                    context.write(new Text("1T" + values[1]),new Text(values[0] + "," + currKey));
                }
            }
        }
    }
    public static class ParametersReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.toString().equals("*")){
                context.write(key,values.iterator().next());
                System.out.println("Real Context" + context.toString());
            }
            else if (key.toString().charAt(1) == 'N'){
                int N = 0;
                Text textOfN = new Text();
                ArrayList<Text> valuesTextArray = new ArrayList<>();
                for (Text value : values){
                    valuesTextArray.add(value);
                    N += 1;
                }
                textOfN.set(key.toString() + String.valueOf(N));
                for (Text value : valuesTextArray){
                    System.out.println("Got here! N VALUE!: " + value.toString() + " Value of Total N: " + textOfN);
                    context.write(value,textOfN);
                }
            }
            else if (key.toString().charAt(1) == 'T'){
                int valueOfT = 0;
                ArrayList<Text> valuesTextArray = new ArrayList<>();
                for (Text value : values){
                    String[] splitValueByComma = value.toString().split(",");
                    int recordValue = Integer.parseInt(splitValueByComma[0]);
                    valueOfT += recordValue;
                    Text keyWithoutStartingNumber = new Text();
                    keyWithoutStartingNumber.set(splitValueByComma[1] + "," + splitValueByComma[2] + "," + splitValueByComma[3]);
                    valuesTextArray.add(keyWithoutStartingNumber);
                }
                Text sendValueOfT = new Text();

                for (Text value: valuesTextArray){
                    sendValueOfT.set(key.toString() + String.valueOf(valueOfT));
                    System.out.println("Got here! T VALUE!: " + value.toString() + " Value of Total T:" + sendValueOfT);
                    context.write(value,sendValueOfT);
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
        //TODO
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}