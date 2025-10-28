---
title: 熟悉常用的 HDFS 操作
author: 刘行逸
date: 20250919
---

# Environments
- M4 MacBook Air
- VMware Fusion Professional Version 13.6.3 (24585314)
- `Linux ubuntuserver 6.14.0-29-generic #29~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Thu Aug 14 17:47:04 UTC 2 aarch64 aarch64 aarch64 GNU/Linux`
- ```text
  java version "21.0.6" 2025-01-21 LTS
  Java(TM) SE Runtime Environment (build 21.0.6+8-LTS-188)
  Java HotSpot(TM) 64-Bit Server VM (build 21.0.6+8-LTS-188, mixed mode, sharing)
  ```
- ```text
  Hadoop 3.4.2
  Source code repository https://github.com/apache/hadoop.git -r e1c0dee881820a4d834ec4a4d2c70d0d953bb933
  Compiled by ahmar on 2025-08-07T15:32Z
  Compiled on platform linux-aarch_64
  Compiled with protoc 3.23.4
  From source with checksum fa94c67d4b4be021b9e9515c9b0f7b6
  This command was run using /usr/local/hadoop/hadoop-3.4.2/share/hadoop/common/hadoop-common-3.4.2.jar
  ```

# Contents & completion
## Contents
### 1
Implement the functions below with code, and accomplist the same tasks using Hadoop's Shell commands

1. Upload any text file to HDFS. If the file already exists in HDFS, let user choose whether to append to the end or overwrite it
2. Download specified file from HDFS. If a local file with the same name already exists, automatically rename it
3. Show the content of a specified HDFS file to the terminal
4. Display the read/write permission, size, creation time, path and other information for a required HDFS file
5. Give a directory in HDFS, list the read/write permission, size, creation time, path and other information for all files in this directory. If an entry is dirctory, recursively list all relevant information under it
6. Give a file path in HDFS, perform creation and deletion operations on this file. If the file's parent directory doesn't exist, automatically create it
7. Give a directory path in HDFS, perform creation and deletion operations on it. When creating the directory, if its parent directory doesn't exist, automatically create the corresponding directory. When deleting the directory, let the user specify whether to delete it's not empty
8. Append specified content to a specified HDFS file. Let the user decide whether to append the content to the beginning or the end of the existing file
9. Delete a specified HDFS file
10. Delete a specified HDFS directory. Let the user decide whether to delete it if it's not empty
11. Move a file from source path to destination path within HDFS

### 2
Implement a class "`MyFSDataInputStream`" with code. It extends "`org.apache.hadoop.fs.FSDataInputStream`" with the following requirements

1. Implement a "`readLine()`" method to read the content of a specified HDFS file line by line. If the end of the file is reached, it should return `null`; otherwise return a line of text from the file content
2. Implement a caching mechanism. When "`MyFSDataInputStream`" is used to read several bytes of data, it should first to check the cache. If the required data is in the cache, provide directly from it; otherwise read from HDFS

Refer to the Java documentation or other resources, and use "`java.net.URL`" and "`org.apache.hadoop.fs.FsURLStreamHandlerFactory`" to programmatically output the texual content of a specified HDFS file to the terminal

## Completion
### Code structure
```text
0919/
├── HadoopOps.sh
├── build.gradle.kts
└── src/
    └── main/
        ├── java/
        │   ├── Entry.java
        │   └── hdfs/
        │       ├── operations/
        │       │   ├── Downloader.java
        │       │   ├── FileReader.java
        │       │   ├── Inspector.java
        │       │   ├── Manager.java
        │       │   └── Uploader.java
        │       └── utils/
        │           ├── Controller.java
        │           ├── HDFSInfo.java
        │           └── MyFSDataInputStream.java
        └── resources/
            ├── core-site.xml
            ├── hdfs-site.xml
            └── log4j.properties
```

### Shell implement
```bash
#!/bin/bash

HDFS_CMD="$HADOOP_HOME/bin/hdfs dfs"

show_menu(){
cat<<EOF
0. Exit
1. Upload
2. Download
3. Read file
4. Inspect info
5. Create (dir end with '/')
6. Delete
7. Move
EOF
}

handleUpload(){
    if [[ (${3} -eq 1) ]] || (! (${HDFS_CMD} -test -e "${2}" 2>/dev/null));then
        ${HDFS_CMD} -test -d $(dirname "${2}") || ${HDFS_CMD} -mkdir -p $(dirname "${2}")
        ${HDFS_CMD} -put -f "${1}" "${2}"
    elif [[ ${3} -eq 0 ]];then
        ${HDFS_CMD} -appendToFile "${1}" "${2}"
    elif [[ ${3} -eq 2 ]];then
        (cat "${1}" <( ${HDFS_CMD} -cat "${2}" )) | (${HDFS_CMD} -put -f - "${2}")
    fi
}

handleDownload(){
    local tarPath="${2}"
    if [[ (${3} -ne 0) && (-e "${tarPath}") ]];then
        local name="${tarPath%%.*}"
        local extName="${tarPath#*.}"
        if [[ "${name}" == "${extName}" ]];then
            extName=""
        else
            extName=".$extName"
        fi
        local cnt=0
        while [[ -e "${name}_${cnt}${extName}" ]];do
            ((++cnt))
        done
        tarPath="${name}_${cnt}${extName}"
        echo "Local path exists, rename to ${tarPath}"
    fi
    ${HDFS_CMD} -get -f "${1}" "${tarPath}"
}

handleRead(){
    ${HDFS_CMD} -cat "${1}"
}

handleInspect(){
    if [[ ${2} -ne 0 ]];then
        ${HDFS_CMD} -ls -R "${1}"
    else
        ${HDFS_CMD} -ls "${1}"
    fi
}

handleCreate(){
    if [[ ${1: -1} == '/' ]];then
        ${HDFS_CMD} -mkdir -p "${1}"
    else
        ${HDFS_CMD} -test -d $(dirname "${1}") || ${HDFS_CMD} -mkdir -p $(dirname "${1}")
        ${HDFS_CMD} -touchz "${1}"
    fi
}

handleDelete(){
    if (${HDFS_CMD} -test -d "${1}" 2>/dev/null) && [[ $(${HDFS_CMD} -ls -R "$1" 2>/dev/null | wc -l) -gt 0 ]];then
        echo "Delete non-empty dir"
    fi
    ${HDFS_CMD} -rm -r "${1}"
}

handleMove(){
    ${HDFS_CMD} -test -d $(dirname "${2}") || ${HDFS_CMD} -mkdir -p $(dirname "${2}")
    ${HDFS_CMD} -mv "${1}" "${2}"
}

handleArgs(){
    case "${1}" in
        upload)
            handleUpload "${2}" "${3}" "${4}"
            ;;
        download)
            handleDownload "${2}" "${3}" "${4}"
            ;;
        read)
            handleRead "${2}"
            ;;
        inspect)
            handleInspect "${2}" "${3}"
            ;;
        create)
            handleCreate "${2}"
            ;;
        delete)
            handleDelete "${2}"
            ;;
        move)
            handleMove "${2}" "${3}"
            ;;
        *)
            echo "Unknown operation: ${1}"
            ;;
    esac
}

interactiveMode(){
    show_menu
    while :
    do
        read -p "Enter choice: " choice
        case "${choice}" in
            0)
                echo "Exit"
                break
                ;;
            1)
                read -p "local path: " localPath
                read -p "hdfs path: " hdfsPath
                read -p "append, overwrite or prepend (0/[1]/2): " mode
                if [[ ${mode} -ne 0 && ${mode} -ne 2 ]];then
                    mode=1
                fi
                handleUpload "${localPath}" "${hdfsPath}" ${mode}
                ;;
            2)
                read -p "hdfs path: " hdfsPath
                read -p "local path: " localPath
                read -p "auto rename if exists? (0/[1]): " rename
                if [[ ${rename} -ne 0 ]];then
                    rename=1
                else
                    rename=0
                fi
                handleDownload "${hdfsPath}" "${localPath}" ${rename}
                ;;
            3)
                read -p "hdfs path: " hdfsPath
                handleRead "${hdfsPath}"
                ;;
            4)
                read -p "hdfs path: " hdfsPath
                read -p "recursive? (0/[1]): " recursive
                if [[ ${recursive} -ne 0 ]];then
                    recursive=1
                else
                    recursive=0
                fi
                handleInspect "${hdfsPath}" ${recursive}
                ;;
            5)
                read -p "hdfs path: " hdfsPath
                handleCreate "${hdfsPath}"
                ;;
            6)
                read -p "hdfs path: " hdfsPath
                handleDelete "${hdfsPath}"
                ;;
            7)
                read -p "source hdfs path: " srcPath
                read -p "destination hdfs path: " destPath
                handleMove "${srcPath}" "${destPath}"
                ;;
            *)
                echo "Unknown choice: ${choice}"
                ;;
        esac
    done
}

if [[ $# -gt 0 ]];then
    handleArgs $@
else
    interactiveMode
fi

```

### Java implement
- `src/main/java/Entry.java`
  ```java
  import hdfs.utils.Controller;
  import hdfs.operations.Uploader;
  import hdfs.operations.Downloader;
  import hdfs.operations.FileReader;
  import hdfs.utils.HDFSInfo;
  import java.util.List;
  import hdfs.operations.Inspector;
  import hdfs.operations.Manager;
  import java.util.Scanner;

  public class Entry{
      private static final String interactiveModeMenu="""
  0. Exit
  1. Upload
  2. Download
  3. Read file
  4. Inspect info
  5. Create (dir end with '/')
  6. Delete
  7. Move
  """;

      private static void handleUpload(String localPath,String hdfsPath,Uploader.Mode mode) throws Exception{
          new Uploader().upload(localPath,hdfsPath,mode);
      }
      private static void handleDownload(String hdfsPath,String localPath,boolean autoRename) throws Exception{
          Downloader.download(hdfsPath,localPath,autoRename);
      }
      private static void handleRead(String hdfsPath) throws Exception{
          String content=FileReader.read(hdfsPath);
          System.out.print(content);
      }
      private static void printInfo(List<Object> hdfsInfos){
          for(Object obj:hdfsInfos)
              if(obj instanceof HDFSInfo)
                  System.out.print(((HDFSInfo)obj).toString());
              else
                  printInfo((List<Object>)obj);
      }
      private static void handleInspect(String hdfsPath,boolean recursive) throws Exception{
          List<Object> hdfsInfos=(Inspector.getInfo(hdfsPath,recursive));
          printInfo(hdfsInfos);
      }
      private static void handleCreate(String hdfsPath) throws Exception{
          new Manager().create(hdfsPath);
      }
      private static void handleDelete(String hdfsPath) throws Exception{
          new Manager().delete(hdfsPath);
      }
      private static void handleMove(String srcPath,String dstPath) throws Exception{
          new Manager().move(srcPath,dstPath);
      }
      private static void handleArgs(String[] args) throws Exception{
          if(args.length<1)
              throw new IllegalArgumentException("Empty arguments");
          String op=args[0];
          switch(op){
              case "upload":
                  if(args.length!=4)
                      throw new IllegalArgumentException("Usage: upload <localPath> <hdfsPath> <a|[o]|p>");
                  Uploader.Mode mode=null;
                  switch(args[3]){
                      case "a":
                          mode=(Uploader.Mode.APPEND);
                          break;
                      case "p":
                          mode=(Uploader.Mode.PREPEND);
                          break;
                      default:
                          mode=(Uploader.Mode.OVERWRITE);
                  }
                  handleUpload(args[1],args[2],mode);
                  break;
              case "download":
                  if(args.length!=4)
                      throw new IllegalArgumentException("Usage: download <hdfsPath> <localPath> <[y]|n>");
                  handleDownload(args[1],args[2],!(args[3].equals("n")));
                  break;
              case "read":
                  if(args.length!=2)
                      throw new IllegalArgumentException("Usage: read <hdfsPath>");
                  handleRead(args[1]);
                  break;
              case "inspect":
                  if(args.length!=3)
                      throw new IllegalArgumentException("Usage: inspect <hdfsPath> <[r]|nr>");
                  handleInspect(args[1],!(args[2].equals("nr")));
                  break;
              case "create":
                  if(args.length!=2)
                      throw new IllegalArgumentException("Usage: create <hdfsPath>");
                  handleCreate(args[1]);
                  break;
              case "delete":
                  if(args.length!=2)
                      throw new IllegalArgumentException("Usage: delete <hdfsPath>");
                  handleDelete(args[1]);
                  break;
              case "move":
                  if(args.length!=3)
                      throw new IllegalArgumentException("Usage: move <srcPath> <dstPath>");
                  handleMove(args[1],args[2]);
                  break;
              default:
                  throw new IllegalArgumentException("Unknown operation: "+op);
          }
      }
      private static void interactiveMode() throws Exception{
          System.out.print(interactiveModeMenu);
          Scanner scanner=new Scanner(System.in);
          boolean running=true;
          while(running)
              try{
                  System.out.print("Enter choice: ");
                  int choice=scanner.nextInt();
                  switch(choice){
                      case 0:
                          running=false;
                          System.out.println("Exit");
                          break;
                      case 1:
                          {
                              System.out.print("local path: ");
                              String localPath=scanner.next();
                              System.out.print("hdfs path: ");
                              String hdfsPath=scanner.next();
                              System.out.print("append, overwrite or prepend (0/[1]/2): ");
                              Uploader.Mode mode=null;
                              switch(scanner.nextInt()){
                                  case 0:
                                      mode=(Uploader.Mode.APPEND);
                                      break;
                                  case 2:
                                      mode=(Uploader.Mode.PREPEND);
                                      break;
                                  default:
                                      mode=(Uploader.Mode.OVERWRITE);
                              }
                              handleUpload(localPath,hdfsPath,mode);
                              break;
                          }
                      case 2:
                          {
                              System.out.print("hdfs path: ");
                              String hdfsPath=scanner.next();
                              System.out.print("local path: ");
                              String localPath=scanner.next();
                              System.out.print("auto rename if exists? (0/[1]): ");
                              if((scanner.nextInt())!=0)
                                  handleDownload(hdfsPath,localPath,true);
                              else
                                  handleDownload(hdfsPath,localPath,false);
                              break;
                          }
                      case 3:
                          {
                              System.out.print("hdfs file: ");
                              String hdfsPath=scanner.next();
                              handleRead(hdfsPath);
                              break;
                          }
                      case 4:
                          {
                              System.out.print("hdfs path: ");
                              String hdfsPath=scanner.next();
                              System.out.print("recursive? (0/[1]): ");
                              if((scanner.nextInt())!=0)
                                  handleInspect(hdfsPath,true);
                              else
                                  handleInspect(hdfsPath,false);
                              break;
                          }
                      case 5:
                          {
                              System.out.print("hdfs path: ");
                              String hdfsPath=scanner.next();
                              handleCreate(hdfsPath);
                              break;
                          }
                      case 6:
                          {
                              System.out.print("hdfs path: ");
                              String hdfsPath=scanner.next();
                              handleDelete(hdfsPath);
                              break;
                          }
                      case 7:
                          {
                              System.out.print("source hdfs path: ");
                              String srcPath=scanner.next();
                              System.out.print("destination hdfs path: ");
                              String dstPath=scanner.next();
                              handleMove(srcPath,dstPath);
                              break;
                          }
                      default:
                          System.err.println("Unknown choice: "+choice);
                  }
              }
              catch(Exception e){
                  System.err.println("Error: "+(e.getMessage()));
                  scanner.nextLine();
              }
          scanner.close();
      }
      public static void main(String[] args){
          try{
              if(args.length>0)
                  handleArgs(args);
              else
                  interactiveMode();
              Controller.closeFS();
          }catch(Exception e){
              System.err.println("Error: "+(e.getMessage()));
          }
      }
  }

  ```
- `src/main/java/hdfs/utils/Controller.java`
  ```java
  package hdfs.utils;

  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.FileSystem;

  public class Controller{
      private static Configuration conf=null;
      private static FileSystem fs=null;

      public static Configuration getConf() throws Exception{
          if(conf==null){
              conf=new Configuration();
          }
          return conf;
      }
      public static FileSystem getFS() throws Exception{
          if(fs==null)
              fs=FileSystem.get(getConf());
          return fs;
      }
      public static void closeFS() throws Exception{
          if(fs!=null){
              fs.close();
              fs=null;
          }
      }
  }

  ```
- `src/main/java/hdfs/utils/HDFSInfo.java`
  ```java
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

  ```
- `src/main/java/hdfs/utils/MyFSDataInputStream.java`
  ```java
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

  ```
- `src/main/java/hdfs/operations/FileReader.java`
  ```java
  package hdfs.operations;

  import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
  import java.net.URL;
  import hdfs.utils.Controller;
  import org.apache.hadoop.fs.FileSystem;
  import org.apache.hadoop.fs.Path;
  import java.io.InputStream;
  import org.apache.hadoop.fs.FSDataInputStream;
  import hdfs.utils.MyFSDataInputStream;

  public class FileReader{
      static{
          try{
              URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
          }catch(Error e){
              System.err.println("Warning: URLStreamHandlerFactory already set: "+e.getMessage());
          }
      }

      public static String read(String hdfsPath) throws Exception{
          FileSystem fs=Controller.getFS();
          Path path=new Path(hdfsPath);
          if(!(fs.exists(path)))
              throw new IllegalArgumentException("Not exists: "+hdfsPath);
          if(fs.isDirectory(path))
              throw new IllegalArgumentException("Is a directory: "+hdfsPath);
          path=path.makeQualified(fs.getUri(),fs.getWorkingDirectory());
          URL url=path.toUri().toURL();
          StringBuilder contentBuilder=new StringBuilder();
          try(InputStream in=(url.openStream());MyFSDataInputStream reader=new MyFSDataInputStream((FSDataInputStream)in)){
              String line;
              while((line=reader.myReadLine())!=null)
                  contentBuilder.append(line).append("\n");
          }catch(Exception e){
              throw e;
          }
          return contentBuilder.toString();
      }
  }

  ```
- `src/main/java/hdfs/operations/Manager.java`
  ```java
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

  ```
- `src/main/java/hdfs/operations/Inspector.java`
  ```java
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
          if(selfStatus.isDirectory())
              for(FileStatus status:statuses)
                  if(recursive)
                      res.addAll(getInfo(status.getPath().toString(),true));
                  else
                      res.add(new HDFSInfo(status));
          return res;
      }
  }

  ```
- `src/main/java/hdfs/operations/Uploader.java`
  ```java
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
      private int appendBufferSize=1024;

      public Uploader(){
          appendBufferSize=1024;
      }
      public Uploader(int bufferSize){
          if(bufferSize<=0)
              throw new IllegalArgumentException("Buffer size must be positive");
          appendBufferSize=bufferSize;
      }
      public void upload(String localPath,String hdfsPath,Mode mode) throws Exception{
          FileSystem fs=(Controller.getFS());
          Path src=new Path(localPath);
          Path dst=new Path(hdfsPath);
          Path tmpSrc=null;
          File file=new File(localPath);
          if(!(file.exists()))
              throw new IllegalArgumentException("Local path not exists: "+localPath);
          if((mode==(Mode.PREPEND))&&(fs.exists(dst))){
              int sep=hdfsPath.indexOf('.');
              sep=(sep==-1)?(hdfsPath.length()):sep;
              int cnt=0;
              do{
                  tmpSrc=new Path(hdfsPath.substring(0,sep)+'_'+cnt+(hdfsPath.substring(sep)));
                  ++cnt;
              }while(!(fs.rename(dst,tmpSrc)));
          }
          if((mode!=(Mode.APPEND))||(!(fs.exists(dst)))){
              fs.copyFromLocalFile(false,true,src,dst);
              if(tmpSrc==null)
                  return;
          }
          try(InputStream in=(mode==(Mode.APPEND))?new BufferedInputStream(new FileInputStream(localPath)):(fs.open(tmpSrc));FSDataOutputStream out=(fs.append(dst))){
              IOUtils.copyBytes(in,out,appendBufferSize,false);
          }catch(Exception e){
              if(mode==(Mode.PREPEND)){
                  fs.delete(dst);
                  fs.rename(tmpSrc,dst);
                  tmpSrc=null;
              }
              throw e;
          }finally{
              if(tmpSrc!=null)
                  fs.delete(tmpSrc);
          }
      }
  }

  ```
- `src/main/java/hdfs/operations/Downloader.java`
  ```java
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

  ```
  
# Problems
1. How to init and build a Java project
2. How to add HDFS configration to the Java project
3. log4j warning:
    ```text
    log4j:WARN No appenders could be found for logger (org.apache.hadoop.util.Shell).
    log4j:WARN Please initialize the log4j system properly.
    log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
    ```
4. `readLine()` method in `src/main/java/hdfs/utils/MyFSDataInputStream.java` error: overridden method is final
5. import `org.apache.hadoop.fs.FsURLStreamHandlerFactory` in `src/main/java/hdfs/operations/FileReader.java` error: cannot find symbol

# Solutions
1. 
	1. Install Gradle and run `gradle init`, follow the instructions (for the course experiment, choose "1: Application" at "Select type of build to generate:", "2: Application and library project" at "Select application structure:", and "1: Kotlin" at "Select build script DSL:")
	2. Edit `settings.gradle.kts` to include this experiment's project
        ```kotlin
        // include("app", "list", "utilities")

        include("0919")
        ```
    3. Use command `./gradlew :0919:clean` to clean this project's build cache; `./gradlew :0919:build` to build this project; `gradle :0919:run --console=plain` to run this project (`--console=plain` is to prevent Gradle's own output from obscuring the program's output); `./gradlew :0919:installDist` and then `0919/build/install/hdfs-app/bin/hdfs-app [args]` for run with arguments
2. 
    1. Put `core-site.xml` and `hdfs-site.xml` in `src/main/resources/`
    2. Edit `build.gradle.kts`
        ```kotlin
        plugins{
            id("buildlogic.java-application-conventions")
        }

        group="Exxon"
        version="1.0"

        application{
            mainClass.set("Entry")
            applicationName="hdfs-app"
        }

        tasks.named<JavaExec>("run"){
            standardInput=System.`in`
            val hadoopHome=System.getenv("HADOOP_HOME")
            if(!hadoopHome.isNullOrBlank()){
                jvmArgs("-Djava.library.path=$hadoopHome/lib/native")
            }
        }

        dependencies{
            implementation("org.apache.hadoop:hadoop-client:3.4.2")
        }

        ```
3. Add `log4j.properties` at `src/main/resources/`
    ```properties
    log4j.rootLogger=WARN,stdout

    log4j.appender.stdout=org.apache.log4j.ConsoleAppender
    log4j.appender.stdout.Target=System.out

    log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
    log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}.%M():%L - %m%n

    ```
4. Since the method `readLine()` is `final` in `DataInputStream`, which `FSDataInputStream` extends, and `MyFSDataInputStream` extends `FSDataInputStream`, so rename it to `myReadLine()`
5. There is no package named `org.apache.hadoop.fs.FsURLStreamHandlerFactory`. The correct one is `org.apache.hadoop.fs.FsUrlStreamHandlerFactory`
