package hdfs.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import java.io.IOException;

public class MyFSDataInputStream extends FSDataInputStream{
    private FSDataInputStream stream;
    private byte[] buffer;
    private int bufferPos=0;
    private int bytesRead=0;

    public MyFSDataInputStream(FSDataInputStream in,int bufferSize){
        super(in.getWrappedStream());
        this.stream=in;
        this.buffer=new byte[bufferSize];
    }
    public MyFSDataInputStream(FSDataInputStream in){
        super(in.getWrappedStream());
        this.stream=in;
        this.buffer=new byte[1024];
    }
    @Override
    public int read() throws IOException{
        if(bufferPos>=bytesRead){
            bytesRead=stream.read(buffer);
            bufferPos=0;
            if(bytesRead<=0)
                return -1;
        }
        return buffer[bufferPos++]&0xFF;
    }
    public String myReadLine() throws IOException{
        StringBuilder sb=new StringBuilder();
        int ch;
        while((ch=read())!=-1){
            if(ch=='\n')
                break;
            else if(ch=='\r')
                continue;
            sb.append((char)ch);
        }
        return (sb.length()>0)?(sb.toString()):null;
    }
}
