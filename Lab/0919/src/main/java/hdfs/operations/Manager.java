package hdfs.operations;

import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Manager{
    public static void create(String hdfsPath) throws Exception{
        FileSystem fs=Controller.getFS();
        Path path=new Path(hdfsPath);
        if(fs.exists(path)){
            System.err.println("Warn: already exists: "+hdfsPath);
            return;
        }
        Path parent=path.getParent();
        if((parent!=null)&&(!fs.exists(parent)))
            if(fs.mkdirs(parent))
                System.out.println("Info: parent dir created: "+parent.toString());
            else
                throw new RuntimeException("Failed to create parent dir: "+parent.toString());
        FSDataOutputStream out=null;
        try{
            out=fs.create(path);
        }catch(Exception e){
            throw e;
        }finally{
            if(out!=null)
                out.close();
        }
    }
    public static void delete(String hdfsPath) throws Exception{
        FileSystem fs=Controller.getFS();
        Path path=new Path(hdfsPath);
        if(!fs.exists(path))
            throw new IllegalArgumentException("File not exists: "+hdfsPath);
        if(!(fs.delete(path,true)))
            throw new RuntimeException("Failed to delete file: "+hdfsPath);
    }
}
