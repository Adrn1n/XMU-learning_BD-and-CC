package hdfs.operations;

import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

public class Uploader{
    public static enum Mode{OVERWRITE,APPEND,PREPEND}
    private int appendBufferSize=1024;

    public Uploader(){
        appendBufferSize=1024;
    }
    public Uploader(int bufferSize){
        if(bufferSize<=0)
            throw new IllegalArgumentException("Buffer size must be positive");
        appendBufferSize=bufferSize;
    }
    public void upload(String localPath,String hdfsPath,Mode mode) throws Exception{
        FileSystem fs=(Controller.getFS());
        Path src=new Path(localPath);
        Path dst=new Path(hdfsPath);
        Path tmpSrc=null;
        File file=new File(localPath);
        if(!(file.exists()))
            throw new IllegalArgumentException("Local path not exists: "+localPath);
        if((mode==(Mode.PREPEND))&&(fs.exists(dst))){
            int sep=hdfsPath.indexOf('.');
            sep=(sep==-1)?(hdfsPath.length()):sep;
            int cnt=0;
            do{
                tmpSrc=new Path(hdfsPath.substring(0,sep)+'_'+cnt+(hdfsPath.substring(sep)));
                ++cnt;
            }while(!(fs.rename(dst,tmpSrc)));
        }
        if((mode!=(Mode.APPEND))||(!(fs.exists(dst)))){
            fs.copyFromLocalFile(false,true,src,dst);
            if(tmpSrc==null)
                return;
        }
        try(InputStream in=(mode==(Mode.APPEND))?new BufferedInputStream(new FileInputStream(localPath)):(fs.open(tmpSrc));FSDataOutputStream out=(fs.append(dst))){
            IOUtils.copyBytes(in,out,appendBufferSize,false);
        }catch(Exception e){
            if(mode==(Mode.PREPEND)){
                fs.delete(dst);
                fs.rename(tmpSrc,dst);
                tmpSrc=null;
            }
            throw e;
        }finally{
            if(tmpSrc!=null)
                fs.delete(tmpSrc);
        }
    }
}
