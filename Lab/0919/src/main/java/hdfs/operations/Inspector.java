package hdfs.operations;

import java.util.List;
import java.util.ArrayList;
import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import hdfs.utils.HDFSInfo;
import java.util.Date;

public class Inspector{
    public static List<Object> getInfo(String hdfsPath,boolean recursive) throws Exception{
        List<Object> res=new ArrayList<>();
        FileSystem fs=(Controller.getFS());
        Path path=new Path(hdfsPath);
        if(!(fs.exists(path)))
            throw new IllegalArgumentException("Not exists: "+hdfsPath);
        FileStatus selfStatus=fs.getFileStatus(path);
        res.add(new HDFSInfo(selfStatus));
        FileStatus[] statuses=(fs.listStatus(path));
        if(selfStatus.isDirectory()&&recursive)
            for(FileStatus status:statuses)
                res.add(getInfo(status.getPath().toString(),recursive));
        return res;
    }
}
