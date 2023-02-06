package top.sugarspace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WordCountJobQueue {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Logger logger = LoggerFactory.getLogger(MyMapper.class);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                Text text = new Text(word);
                LongWritable longWritable = new LongWritable(1L);
                // logger.info("k2:" + word + "...v2:1");
                context.write(text, longWritable);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final Logger logger = LoggerFactory.getLogger(MyReducer.class);

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                // logger.info("<k2,v2>=<" + key.toString() + "," + value.get() + ">");
                sum += value.get();
            }
            LongWritable l = new LongWritable(sum);
            // logger.info("<k3,v3>=<" + key.toString() + "," + l.get() + ">");
            context.write(key, l);
        }
    }

    public static void main(String[] args) {
        try {
            // 配置
            Configuration configuration = new Configuration();

            //解析命令行中-D后面传递过来的参数，添加到conf中
            String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

            // Job
            Job job = Job.getInstance(configuration);

            //
            job.setJarByClass(WordCountJobQueue.class);

            FileInputFormat.setInputPaths(job, new Path(remainingArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.waitForCompletion(true);

        } catch (Exception e) {

        }
    }

}
