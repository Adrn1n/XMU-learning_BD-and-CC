package hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Manager{
    private static Configuration conf=null;
    private static FileSystem fs=null;

    public static Configuration getConf() throws Exception{
        if(conf==null){
            conf=new Configuration();
        }
        return conf;
    }
    public static FileSystem getFS() throws Exception{
        if(fs==null)
            fs=FileSystem.get(getConf());
        return fs;
    }
    public static void closeFS() throws Exception{
        if(fs!=null){
            fs.close();
            fs=null;
        }
    }
}
