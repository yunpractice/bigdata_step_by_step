import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxNumber {
    // Map: 将输入的文本数据转换为<word-1>的键值对
    public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

            int max = 0;
            String s = null;
            String line = value.toString().toLowerCase();

            //split all words of line
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                s = tokenizer.nextToken();
                int tmp = Integer.parseInt(s.toString());
                if(tmp > max){
                    max = tmp;
                }
            }
            context.write(new Text("max"), new IntWritable(max));
        }
    }

    // Reduce: add all word-counts for a key
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, Text> {
        int min_num = 0;

        /**
         * minimum showing words
         * */
        public void setup(Context context) {
        }

        /**
         * reduce
         * */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            int max = 0;
            for (IntWritable val : values) {
                int tmp = val.get();
                if(tmp > max){
                    max = tmp;
                }
            }
            context.write(key, new Text(Integer.toString(max)));
        }
    }

    /**
     * IntWritable comparator
     * */
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
     * main: run two job
     * */
    public static void main(String[] args){
        boolean exit = false;

        Configuration conf = new Configuration();
        try{
            /**
             * run first-round to count
             * */
            Job job = new Job(conf, "jiq-wordcountjob-1");
            job.setJarByClass(MaxNumber.class);

            //set format of input-output
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //set class of output's key-value of MAP
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //set mapper and reducer
            job.setMapperClass(WordCountMap.class);     
            job.setReducerClass(WordCountReduce.class);
            //job.setCombinerClass(WordCountReduce.class);

            //set path of input-output
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);        
        }catch(Exception e){
            e.printStackTrace();
        }finally{
                if(exit) System.exit(1);
                System.exit(0);
        }
    }

}

