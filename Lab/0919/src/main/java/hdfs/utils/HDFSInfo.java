package hdfs.utils;

import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import java.util.LinkedHashMap;

public class HDFSInfo{
    private Map<String,Object> info=new LinkedHashMap<>();

    public HDFSInfo(FileStatus status){
        info.put("Path",status.getPath());
        info.put("Size",status.getLen());
        info.put("Permissions",status.getPermission());
        info.put("Modification time",new java.util.Date(status.getModificationTime()));
    }
    public Map<String,Object> getInfo(){
        return info;
    }
    public String toString(){
        StringBuilder sb=new StringBuilder();
        info.forEach((k, v)->sb.append(k).append(": ").append(v).append("\n"));
        return sb.toString();
    }
}
