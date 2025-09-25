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
    private static final int appendBufferSize=1024;

    public static void upload(String localPath,String hdfsPath,Mode mode) throws Exception{
        FileSystem fs=(Controller.getFS());
        Path src=new Path(localPath);
        Path dst=new Path(hdfsPath);
        Path tmpDst=null;
        File file=new File(localPath);
        if(!(file.exists()))
            throw new IllegalArgumentException("Local path not exists: "+localPath);
        if((mode==(Mode.PREPEND))&&(fs.exists(dst))){
            String baseName=dst.getName();
            int sep=baseName.indexOf('.');
            sep=(sep==-1)?(baseName.length()):sep;
            int cnt=0;
            do{
                tmpDst=new Path(dst.getParent(),(baseName.substring(0,sep))+'_'+cnt+(baseName.substring(sep)));
                ++cnt;
            }while(!(fs.rename(dst,tmpDst)));
        }
        if((mode!=(Mode.APPEND))||(!(fs.exists(dst)))){
            fs.copyFromLocalFile(false,true,src,dst);
            if(tmpDst==null)
                return;
        }
        try(InputStream in=(mode==(Mode.APPEND))?new BufferedInputStream(new FileInputStream(localPath)):(fs.open(tmpDst));FSDataOutputStream out=(fs.append(dst))){
            IOUtils.copyBytes(in,out,appendBufferSize,false);
        }catch(Exception e){
            if(mode==(Mode.PREPEND)){
                fs.delete(dst);
                fs.rename(tmpDst,dst);
                tmpDst=null;
            }
            throw e;
        }finally{
            if(tmpDst!=null)
                fs.delete(tmpDst);
        }
    }
}
