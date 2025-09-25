package hdfs.operations;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import java.net.URL;
import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import hdfs.utils.MyFSDataInputStream;

public class FileReader{
    static{
        try{
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        }catch(Error e){
            System.err.println("Warning: URLStreamHandlerFactory already set: "+e.getMessage());
        }
    }

    public static String read(String hdfsPath) throws Exception{
        FileSystem fs=Controller.getFS();
        Path path=new Path(hdfsPath);
        if(!(fs.exists(path)))
            throw new IllegalArgumentException("Not exists: "+hdfsPath);
        if(fs.isDirectory(path))
            throw new IllegalArgumentException("Is a directory: "+hdfsPath);
        path=path.makeQualified(fs.getUri(),fs.getWorkingDirectory());
        URL url=path.toUri().toURL();
        StringBuilder contentBuilder=new StringBuilder();
        try(InputStream in=(url.openStream());MyFSDataInputStream reader=new MyFSDataInputStream((FSDataInputStream)in)){
            String line;
            while((line=reader.myReadLine())!=null)
                contentBuilder.append(line).append("\n");
        }catch(Exception e){
            throw e;
        }
        return contentBuilder.toString();
    }
}
