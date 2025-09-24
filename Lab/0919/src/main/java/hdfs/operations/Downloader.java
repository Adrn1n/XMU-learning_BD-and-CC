package hdfs.operations;

import hdfs.utils.Manager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;

public class Downloader{
    public static void download(String hdfsPath,String localPath) throws Exception{
        FileSystem fs=Manager.getFS();
        Path src=new Path(hdfsPath);
        File dst=new File(localPath);
        if(!fs.exists(src))
            throw new IllegalArgumentException("HDFS file not exists: "+hdfsPath);
        if(dst.exists()){
            String baseName=dst.getName();
            int sep=baseName.indexOf('.');
            sep=(sep==-1)?baseName.length():sep;
            String parent=dst.getParent();
            parent=(parent==null)?"":parent+File.separator;
            int cnt=0;
            do{
                dst=new File(parent+baseName.substring(0,sep)+'_'+cnt+baseName.substring(sep));
                ++cnt;
            }while(dst.exists());
            System.out.println("Local file exists, rename to "+dst.getAbsolutePath());
        }
        fs.copyToLocalFile(false,src,new Path(dst.toURI()),true);
    }
}
