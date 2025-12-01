package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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

public class FileSortWithRank extends Configured implements Tool{
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,NullWritable>{
        public void map(LongWritable key,Text value,OutputCollector<IntWritable,NullWritable> output,Reporter reporter) throws IOException{
            int num=Integer.parseInt(value.toString().trim());
            output.collect(new IntWritable(num),NullWritable.get());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<IntWritable,NullWritable,Text,NullWritable>{
        private int rank=1;

        public void reduce(IntWritable key,Iterator<NullWritable> values,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(new Text((rank++)+" "+(key.get())),NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        int len=args.length;
        if(len>1){
            JobConf conf=new JobConf(getConf(),FileSortWithRank.class);
            conf.setJobName("File sort with rank");
            conf.setMapOutputKeyClass(IntWritable.class);
            conf.setMapOutputValueClass(NullWritable.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(NullWritable.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setJarByClass(FileSortWithRank.class);
            conf.setNumReduceTasks(1);
            for(int i=0;i<(len-1);i++)
                FileInputFormat.addInputPath(conf,new Path(args[i]));
            FileOutputFormat.setOutputPath(conf,new Path(args[len-1]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileSortWithRank: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileSortWithRank(),args);
    }
}
