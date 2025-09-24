package hdfs.operations;

import java.util.Map;
import hdfs.utils.Manager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.util.HashMap;
import java.util.Date;

public class Inspector{
    public static Map<String,Object> getInfo(String hdfsPath) throws Exception{
        FileSystem fs=Manager.getFS();
        Path path=new Path(hdfsPath);
        if(!fs.exists(path))
            throw new IllegalArgumentException("HDFS path not exists: "+hdfsPath);
        FileStatus status=fs.getFileStatus(path);
        Map<String,Object> info=new HashMap<>();
        info.put("Path",status.getPath());
        info.put("Size",status.getLen());
        info.put("Permissions",status.getPermission());
        info.put("Modification Time",new Date(status.getModificationTime()));
        return info;
    }
}
