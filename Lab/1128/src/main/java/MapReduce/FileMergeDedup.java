package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.mapred.Reducer;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;

public class FileMergeDedup extends Configured implements Tool{
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,NullWritable>{
        public void map(LongWritable key,Text value,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(value,NullWritable.get());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<Text,NullWritable,Text,NullWritable>{
        public void reduce(Text key,Iterator<NullWritable> values,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(key,NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        if(args.length==3){
            JobConf conf=new JobConf(getConf(),FileMergeDedup.class);
            conf.setJobName("File merge and deduplication");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(NullWritable.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setCombinerClass(LineReducer.class);
            conf.setJarByClass(FileMergeDedup.class);
            FileInputFormat.addInputPath(conf,new Path(args[0]));
            FileInputFormat.addInputPath(conf,new Path(args[1]));
            FileOutputFormat.setOutputPath(conf,new Path(args[2]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileMergeDedup: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileMergeDedup(),args);
    }
}
