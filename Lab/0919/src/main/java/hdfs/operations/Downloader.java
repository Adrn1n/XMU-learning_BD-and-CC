package hdfs.operations;

import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;

public class Downloader{
    public static void download(String hdfsPath,String localPath,boolean autoRename) throws Exception{
        FileSystem fs=(Controller.getFS());
        Path src=new Path(hdfsPath);
        File dst=new File(localPath);
        if(!(fs.exists(src)))
            throw new IllegalArgumentException("HDFS path not exists: "+hdfsPath);
        if(autoRename&&(dst.exists())){
            int sep=(localPath.indexOf('.'));
            sep=(sep==-1)?(localPath.length()):sep;
            int cnt=0;
            do{
                dst=new File(localPath.substring(0,sep)+'_'+cnt+(localPath.substring(sep)));
                ++cnt;
            }while(dst.exists());
            System.out.println("Local path exists, rename to "+(dst.getPath()));
        }
        fs.copyToLocalFile(false,src,new Path(dst.toURI()),true);
    }
}
