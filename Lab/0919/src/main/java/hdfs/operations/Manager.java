package hdfs.operations;

import hdfs.utils.Controller;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;

public class Manager{
    private FileSystem fs;

    public Manager() throws Exception{
        fs=Controller.getFS();
    }
    public void create(String hdfsPath) throws Exception{
        Path path=new Path(hdfsPath);
        if(fs.exists(path)){
            System.err.println("Warn: already exists: "+hdfsPath);
            return;
        }
        if(hdfsPath.endsWith("/"))
            if(fs.mkdirs(path))
                return;
            else
                throw new RuntimeException("Failed to create dir: "+hdfsPath);
        else{
            Path parent=path.getParent();
            if((parent!=null)&&(!(fs.exists(parent))))
                if(!(fs.mkdirs(parent)))
                    throw new RuntimeException("Failed to create parent dir: "+parent.toString());
            FSDataOutputStream out=null;
            try{
                out=(fs.create(path));
            }catch(Exception e){
                throw e;
            }finally{
                if(out!=null)
                    out.close();
            }
        }
    }
    public void delete(String hdfsPath) throws Exception{
        Path path=new Path(hdfsPath);
        if(!(fs.exists(path)))
            System.err.println("Warn: not exists: "+hdfsPath);
        if(fs.isDirectory(path)){
            FileStatus[] statuses=(fs.listStatus(path));
            if((statuses!=null)&&(statuses.length>0))
                System.out.println("Delete non-empty dir: "+hdfsPath);
        }
        if(!(fs.delete(path,true)))
            throw new RuntimeException("Failed to delete: "+hdfsPath);
    }
    public void move(String srcPath,String dstPath) throws Exception{
        Path src=new Path(srcPath);
        if(!(fs.exists(src)))
            throw new IllegalArgumentException("Source not exists: "+srcPath);
        Path dst=new Path(dstPath);
        if(fs.exists(dst))
            throw new IllegalArgumentException("Destination exists: "+dstPath);
        Path parent=dst.getParent();
        if((parent!=null)&&(!(fs.exists(parent))))
            if(!(fs.mkdirs(parent)))
                throw new RuntimeException("Failed to create parent dir: "+parent.toString());
        if(!(fs.rename(src,dst)))
            throw new RuntimeException("Failed to move file from "+srcPath+" to "+dstPath);
    }
}
