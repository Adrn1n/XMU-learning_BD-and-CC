---
title: MapReduce初级编程实践
author: 刘行逸
date: 20251128
---

# Environments
- M4 MacBook Air
- VMware Fusion Professional Version 13.6.3 (24585314)
- `Linux ubuntuserver 6.14.0-36-generic #36~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Wed Oct 15 15:22:32 UTC 2 aarch64 aarch64 aarch64 GNU/Linux`
- 
    ```text
    java version "11.0.24" 2024-07-16 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.24+7-LTS-271)
    Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.24+7-LTS-271, mixed mode)
    ```
- 
    ```text
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
### (1) Implement file merging and duplicate removal
For two input files, File A and File B, write a MapReduce program to merge the two files and remove duplicate records, producing a new output file C.
The following are sample inputs and outputs.

Sample input file A:
```text
	20170101     x
	20170102     y
	20170103     x
	20170104     y
	20170105     z
20170106     x
```

Sample input file B:
```text
20170101      y
20170102      y
20170103      x
20170104      z
20170105      y
```

Merged output file C after removing duplicates:
```text
20170101      x
20170101      y
20170102      y
20170103      x
20170104      y
20170104      z
20170105      y
	20170105      z
20170106      x
```

### (2) Write a program to sort the input files
Multiple input files are given, and each line in each file contains one integer.
Read all integers from all files, sort them in ascending order, and output them into a new file.
The output format is two integers per line:
- the first integer is the rank in the sorted sequence
- the second integer is the original integer

Sample input file 1:
```text
33
37
12
40
```

Sample input file 2:
```text
4
16
39
5
```

Sample input file 3:
```text
1
45
25
```

Output file:
```text
1 1
2 4
3 5
4 12
5 16
6 25
7 33
8 37
9 39
10 40
11 45
```

### (3) Mine information from a given table
A child–parent table is provided.
You are required to extract the relationships and generate a grandchild–grandparent table.

Input file:
```text
	child          parent
	Steven        Lucy
	Steven        Jack
	Jone         Lucy
	Jone         Jack
	Lucy         Mary
	Lucy         Frank
	Jack         Alice
	Jack         Jesse
	David       Alice
	David       Jesse
	Philip       David
	Philip       Alma
	Mark       David
Mark       Alma
```

Output file:
```text
	grandchild       grandparent
	Steven          Alice
	Steven          Jesse
	Jone            Alice
	Jone            Jesse
	Steven          Mary
	Steven          Frank
	Jone            Mary
	Jone            Frank
	Philip           Alice
	Philip           Jesse
	Mark           Alice
Mark           Jesse
```

## Completion
`build.gradle.kts`
```kotlin
plugins{
    id("buildlogic.java-application-conventions")
}

group="Exxon"
version="1.0"

application{
    mainClass.set("Entry")
    applicationName="MapReduce-app"
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

`Entry.java`
```java
import java.util.Scanner;
import MapReduce.FileMergeDedup;
import MapReduce.FileSortWithRank;
import MapReduce.FileGrandRelations;
import java.util.Arrays;

public class Entry{
    private static final String interactiveModeMenu="0. Exit\n1. File merge with deduplication\n2. File sort with rank\n3. Grand relations\n";

    private static void handleMD(String[] args) throws Exception{
        FileMergeDedup.main(args);
    }
    private static void handleSWR(String[] args) throws Exception{
        FileSortWithRank.main(args);
    }
    private static void handleGR(String[] args) throws Exception{
        FileGrandRelations.main(args);
    }
    private static void handleArgs(String[] args) throws Exception{
        if(args.length>0){
            switch(args[0]){
                case "MD":
                    if(args.length==4){
                        handleMD(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: MD <inputFile1> <inputFile2> <outputFile>");
                case "SWR":
                    if(args.length>2){
                        handleSWR(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: SWR <inputFile1> [<inputFile2> ...] <outputFile>");
                case "GR":
                    if(args.length==3){
                        handleGR(Arrays.copyOfRange(args,1,args.length));
                        break;
                    }
                    throw new IllegalArgumentException("handleArgs(): Usage: GR <inputFile> <output>");
                default:
                    throw new IllegalArgumentException("handleArgs(): Unknown command: "+args[0]);
            }
            return;
        }
        throw new IllegalArgumentException("handleArgs(): Empty args");
    }
    private static void interactiveMode() throws Exception{
        System.out.print(interactiveModeMenu);
        Scanner scanner=new Scanner(System.in);
        boolean running=true;
        while(running)
            try{
                System.out.print("Enter choice: ");
                int choice=scanner.nextInt();
                scanner.nextLine();
                switch(choice){
                    case 0:
                        running=false;
                        System.out.println("Exit");
                        break;
                    case 1:
                        String[] Files1=new String[3];
                        System.out.print("Input file1 path: ");
                        Files1[0]=scanner.nextLine();
                        System.out.print("Input file2 path: ");
                        Files1[1]=scanner.nextLine();
                        System.out.print("Output file path: ");
                        Files1[2]=scanner.nextLine();
                        handleMD(Files1);
                        break;
                    case 2:
                        System.out.print("Number of input files: ");
                        int n=scanner.nextInt();
                        scanner.nextLine();
                        String[] Files2=new String[n+1];
                        for(int i=0;i<n;++i){
                            System.out.print("Input file "+(i+1)+" path: ");
                            Files2[i]=scanner.nextLine();
                        }
                        System.out.print("Output file path: ");
                        Files2[n]=scanner.nextLine();
                        handleSWR(Files2);
                        break;
                    case 3:
                        String[] Files3=new String[2];
                        System.out.print("Input file path: ");
                        Files3[0]=scanner.nextLine();
                        System.out.print("Output file path: ");
                        Files3[1]=scanner.nextLine();
                        handleGR(Files3);
                        break;
                    default:
                        System.err.println("Invalid choice: "+choice);
                }
            }catch(Exception e){
                System.out.println("interactiveMode(): "+e.getMessage());
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
        }catch(Exception e){
            System.err.println("main(): "+e.getMessage());
        }
    }
}

```

### (1) Implement file merging and duplicate removal
`MapReduce/FileMergeDedup.java`
```java
package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.mapred.Reducer;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;

public class FileMergeDedup extends Configured implements Tool{
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,NullWritable>{
        public void map(LongWritable key,Text value,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(value,NullWritable.get());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<Text,NullWritable,Text,NullWritable>{
        public void reduce(Text key,Iterator<NullWritable> values,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(key,NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        if(args.length==3){
            JobConf conf=new JobConf(getConf(),FileMergeDedup.class);
            conf.setJobName("File merge and deduplication");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(NullWritable.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setCombinerClass(LineReducer.class);
            conf.setJarByClass(FileMergeDedup.class);
            FileInputFormat.addInputPath(conf,new Path(args[0]));
            FileInputFormat.addInputPath(conf,new Path(args[1]));
            FileOutputFormat.setOutputPath(conf,new Path(args[2]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileMergeDedup: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileMergeDedup(),args);
    }
}

```

### (2) Write a program to sort the input files
`MapReduce/FileSortWithRank.java`
```java
package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.mapred.Reducer;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;

public class FileSortWithRank extends Configured implements Tool{
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,NullWritable>{
        public void map(LongWritable key,Text value,OutputCollector<IntWritable,NullWritable> output,Reporter reporter) throws IOException{
            int num=Integer.parseInt(value.toString().trim());
            output.collect(new IntWritable(num),NullWritable.get());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<IntWritable,NullWritable,Text,NullWritable>{
        private int rank=1;

        public void reduce(IntWritable key,Iterator<NullWritable> values,OutputCollector<Text,NullWritable> output,Reporter reporter) throws IOException{
            output.collect(new Text((rank++)+" "+(key.get())),NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        int len=args.length;
        if(len>1){
            JobConf conf=new JobConf(getConf(),FileSortWithRank.class);
            conf.setJobName("File sort with rank");
            conf.setMapOutputKeyClass(IntWritable.class);
            conf.setMapOutputValueClass(NullWritable.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(NullWritable.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setJarByClass(FileSortWithRank.class);
            conf.setNumReduceTasks(1);
            for(int i=0;i<(len-1);i++)
                FileInputFormat.addInputPath(conf,new Path(args[i]));
            FileOutputFormat.setOutputPath(conf,new Path(args[len-1]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileSortWithRank: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileSortWithRank(),args);
    }
}

```

### (3) Mine information from a given table
`MapReduce/FileGrandRelations.java`
```java
package MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.mapred.Reducer;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;

public class FileGrandRelations extends Configured implements Tool{
    private static final int tag_length=3;
    private static class LineMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
        private boolean isFirst=true;

        public void map(LongWritable key,Text value,OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
            if(isFirst){
                isFirst=false;
                return;
            }
            String[] tokens=value.toString().split("\\s+");
            if(tokens.length==2){
                output.collect(new Text(tokens[1]),new Text("c: "+tokens[0]));
                output.collect(new Text(tokens[0]),new Text("p: "+tokens[1]));
                return;
            }
            System.err.println("FileGrandRelations: LineMapper: map(): Invalid line: "+value.toString());
        }
    }
    private static class LineReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
        private boolean isFirst=true;

        public void reduce(Text key,Iterator<Text> values,OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
            if(isFirst){
                output.collect(new Text("grandchild"),new Text("grandparent"));
                isFirst=false;
            }
            ArrayList<String> children=new ArrayList<String>();
            ArrayList<String> parents=new ArrayList<String>();
            while(values.hasNext()){
                String relation=values.next().toString();
                if(relation.startsWith("c: "))
                    children.add(relation.substring(tag_length));
                else if(relation.startsWith("p: "))
                    parents.add(relation.substring(tag_length));
                else
                    System.err.println("FileGrandRelations: LineReducer: reduce(): Invalid relation: "+relation);
            }
            for(String child:children)
                for(String parent:parents)
                    output.collect(new Text(child),new Text(parent));
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        if(args.length==2){
            JobConf conf=new JobConf(getConf(),FileGrandRelations.class);
            conf.setJobName("File grand relations");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(LineMapper.class);
            conf.setReducerClass(LineReducer.class);
            conf.setJarByClass(FileGrandRelations.class);
            FileInputFormat.addInputPath(conf,new Path(args[0]));
            FileOutputFormat.setOutputPath(conf,new Path(args[1]));
            JobClient.runJob(conf);
            return 0;
        }
        System.err.println("FileGrandRelations: run(): Invalid arguments");
        ToolRunner.printGenericCommandUsage(System.err);
        return -1;
    }
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new FileGrandRelations(),args);
    }
}

```

# Problems
1. System downgrade Java to version 11 and is incompatible with previous gradle settings
2. `failed on connection exception` with Hadoop shell commands

# Solutions
1. 
    - `Lab/gradle/wrapper/gradle-wrapper.properties`
        ```properties
        #//...
        # distributionUrl=https\://services.gradle.org/distributions/gradle-9.1.0-bin.zip
        distributionUrl=https\://services.gradle.org/distributions/gradle-7.6.6-bin.zip
        #//...
        ```
    - `Lab/settings.gradle.kts`
        ```kotlin
        //...
        //id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
        id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
        //...
        ```
    - `Lab/buildSrc/src/main/kotlin/buildlogic.java-common-conventions.gradle.kts`
        ```kotlin
        //...
        //languageVersion = JavaLanguageVersion.of(21)
        languageVersion.set(JavaLanguageVersion.of(11))
        //...
        ```
2. 
    - `Lab/1128/src/main/resources/core-site.xml`
        ```xml
        <!-- ... -->
        <!-- <value>hdfs://localhost:9000</value> -->
        <value>hdfs://ubuntuserver:9000</value>
        <!-- ... -->
        ```
    - `Lab/1128/src/main/resources/hdfs-site.xml`
        ```xml
        <!-- ... -->
        <property>
            <name>dfs.namenode.rpc-bind-host</name>
            <value>0.0.0.0</value>
        </property>
        <property>
            <name>dfs.namenode.http-bind-host</name>
            <value>0.0.0.0</value>
        </property>
        <!-- ... -->
        ```
