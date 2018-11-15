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

public class SortNumber {
    /**
     * Map: 将输入的文本数据转换为<word-1>的键值对
     * */
    public static class SortNumberMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        IntWritable n = new IntWritable(0);
        final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

            String s = null;
            String line = value.toString().toLowerCase();

            //split all words of line
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                s = tokenizer.nextToken();
                n.set(Integer.parseInt(s));
                context.write(n, one);
            }
        }
    }

    public static class SortNumberReduce extends Reducer<IntWritable, IntWritable, Text, Text> {
        private static IntWritable linenum = new IntWritable(1);
        
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            context.write(new Text(Integer.toString(linenum.get())),new Text(Integer.toString(key.get())));
            linenum.set(linenum.get() + 1);
        }
    }

    public static void main(String[] args){
        Configuration conf = new Configuration();
        try{
            FileSystem.get(conf).deleteOnExit(new Path(args[1]));
            
            Job job = new Job(conf, "sort_number");
            job.setJarByClass(SortNumber.class);

            //set format of input-output
            job.setInputFormatClass(TextInputFormat.class);
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            //set class of output's key-value of MAP
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //set mapper and reducer
            job.setMapperClass(SortNumberMap.class);     
            job.setReducerClass(SortNumberReduce.class);

            //set path of input-output
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);        
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            System.exit(0);
        }
    }
}

