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
                word.set(currKey);
                String[] values = val.split(",");
                if ("m_X,pleased,_ADP_".equals(currKey)){
                    System.out.println(new Text("0N" + values[0]) + " value: " + word);
                    System.out.println(new Text("1N" + values[1])+ " value: " + word);
                    System.out.println(new Text("0T" + values[0])+ " value: " + values[1] + "," + currKey);
                    System.out.println(new Text("1N" + values[1])+ " value: " + values[0] + "," + currKey);
                }
                context.write(new Text("0N" + values[0]),word);
                context.write(new Text("1N" + values[1]),word);
                context.write(new Text("0T" + values[0]),new Text(values[1] + "," + currKey));
                context.write(new Text("1T" + values[1]),new Text(values[0] + "," + currKey));
            }
        }
    }
    //0NV   m_X,pleased,_ADP_
    public static class ParametersReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.toString().charAt(1) == 'N'){
                int N = 0;
                int i = 0;
                Text textOfN = new Text();
                if (key.toString().equals("0N10")) {
                    System.out.println("changed");
                    i = 1;
                }
                ArrayList<String> valuesTextArray = new ArrayList<>();
                for (Text value : values){
                    if (i==1){
                        System.out.println("value: " + value.toString() + "key: " + key.toString());
                    }
                    valuesTextArray.add(value.toString());
                    N += 1;
                }
                textOfN.set(key.toString().substring(0,2) + N);
                for (String value : valuesTextArray){
                    if (i==1){
                        System.out.println("value To Enter: " + value.toString() + "key: " + key.toString());
                    }
                    context.write(new Text(value),textOfN);
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
                    sendValueOfT.set(key.toString().substring(0,2) + String.valueOf(valueOfT));
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
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}