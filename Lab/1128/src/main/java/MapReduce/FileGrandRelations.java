package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.mapred.Reducer;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;

public class FileGrandRelations extends Configured implements Tool{
    private static final int tag_length=3;
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
        private boolean isFirst=true;

        public void map(LongWritable key,Text value,OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
            if(isFirst){
                isFirst=false;
                return;
            }
            String[] tokens=value.toString().split("\\s+");
            if(tokens.length==2){
                output.collect(new Text(tokens[1]),new Text("c: "+tokens[0]));
                output.collect(new Text(tokens[0]),new Text("p: "+tokens[1]));
                return;
            }
            System.err.println("FileGrandRelations: LineMapper: map(): Invalid line: "+value.toString());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
        private boolean isFirst=true;

        public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
            if(isFirst){
                output.collect(new Text("grandchild"),new Text("grandparent"));
                isFirst=false;
            }
            ArrayList<String> children=new ArrayList<String>();
            ArrayList<String> parents=new ArrayList<String>();
            while(values.hasNext()){
                String relation=values.next().toString();
                if(relation.startsWith("c: "))
                    children.add(relation.substring(tag_length));
                else if(relation.startsWith("p: "))
                    parents.add(relation.substring(tag_length));
                else
                    System.err.println("FileGrandRelations: LineReducer: reduce(): Invalid relation: "+relation);
            }
            for(String child:children)
                for(String parent:parents)
                    output.collect(new Text(child),new Text(parent));
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        if(args.length==2){
            JobConf conf=new JobConf(getConf(),FileGrandRelations.class);
            conf.setJobName("File grand relations");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setJarByClass(FileGrandRelations.class);
            FileInputFormat.addInputPath(conf,new Path(args[0]));
            FileOutputFormat.setOutputPath(conf,new Path(args[1]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileGrandRelations: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileGrandRelations(),args);
    }
}
