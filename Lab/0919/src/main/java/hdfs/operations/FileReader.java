package hdfs.operations;

import hdfs.utils.Manager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class FileReader{
    public static String read(String hdfsPath) throws Exception{
        FileSystem fs=Manager.getFS();
        Path path=new Path(hdfsPath);
        if(!fs.exists(path))
            throw new IllegalArgumentException("HDFS file not exists: "+hdfsPath);
        if(fs.isDirectory(path))
            throw new IllegalArgumentException("HDFS path is a directory: "+hdfsPath);
        StringBuilder contentBuilder=new StringBuilder();
        FSDataInputStream in=null;
        BufferedReader reader=null;
        try{
            in=fs.open(path);
            reader=new BufferedReader(new InputStreamReader(in));
            String line;
            while((line=reader.readLine())!=null)
                contentBuilder.append(line).append("\n");
        }catch(Exception e){
            throw e;
        }
        finally{
            if(reader!=null)
                reader.close();
            if(in!=null)
                in.close();
        }
        return contentBuilder.toString();
    }
}
