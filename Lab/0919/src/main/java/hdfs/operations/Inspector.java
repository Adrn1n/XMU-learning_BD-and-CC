package hdfs.operations;

import hdfs.utils.HDFSInfo;
import hdfs.utils.Manager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.util.Date;

public class Inspector{
    public static HDFSInfo getInfo(String hdfsPath) throws Exception{
        FileSystem fs=Manager.getFS();
        Path path=new Path(hdfsPath);
        if(!fs.exists(path))
            throw new IllegalArgumentException("HDFS path not exists: "+hdfsPath);
        FileStatus status=fs.getFileStatus(path);
        HDFSInfo hdfsInfo=new HDFSInfo();
        hdfsInfo.info.put("Path",status.getPath());
        hdfsInfo.info.put("Size",status.getLen());
        hdfsInfo.info.put("Permissions",status.getPermission());
        hdfsInfo.info.put("Modification Time",new Date(status.getModificationTime()));
        return hdfsInfo;
    }
}
