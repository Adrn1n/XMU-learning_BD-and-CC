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
        if(dst.exists())
            if(autoRename){
                String baseName=(dst.getName());
                int sep=(baseName.indexOf('.'));
                sep=(sep==-1)?(baseName.length()):sep;
                int cnt=0;
                do{
                    dst=new File(dst.getParent(),(baseName.substring(0,sep))+'_'+cnt+(baseName.substring(sep)));
                    ++cnt;
                }while(dst.exists());
                System.out.println("Local path exists, rename to "+(dst.getPath()));
            }else
                throw new IllegalArgumentException("Local path exists: "+localPath);
        fs.copyToLocalFile(false,src,new Path(dst.toURI()),true);
    }
}
