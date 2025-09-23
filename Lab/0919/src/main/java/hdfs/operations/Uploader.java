package hdfs.operations;

import hdfs.utils.Manager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Uploader{
    private static final int appendBufferSize=1024;

    public static void upload(String localPath,String hdfsPath,boolean append) throws Exception{
        FileSystem fs=Manager.getFS();
        Path src=new Path(localPath);
        Path dst=new Path(hdfsPath);
        File localFile=new File(localPath);
        if(!localFile.exists())
            throw new IllegalArgumentException("Local file not exists: "+localPath);
        if((!fs.exists(dst))||(!append))
            fs.copyFromLocalFile(false,true,src,dst);
        else{
            BufferedInputStream in=new BufferedInputStream(new FileInputStream(localFile));
            FSDataOutputStream out=fs.append(dst);
            byte[] buffer=new byte[appendBufferSize];
            int bytesRead=0;
            while((bytesRead=in.read(buffer))>0)
                out.write(buffer,0,bytesRead);
            out.close();
            in.close();
        }
    }
}
