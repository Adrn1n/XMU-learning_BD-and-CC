---
title: Spark初级编程实践
author: 刘行逸
date: 20251212
---

# Environments
- M4 MacBook Air
- VMware Fusion Professional Version 13.6.3 (24585314)
- Linux ubuntuserver 6.14.0-37-generic #37~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Fri Nov 21 03:10:52 UTC 2 aarch64 aarch64 aarch64 GNU/Linux
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
- 
    ```text
    Welcome to
        ____              __
        / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
    /___/ .__/\_,_/_/ /_/\_\   version 3.5.7
        /_/
                            
    Using Scala version 2.12.18, Java HotSpot(TM) 64-Bit Server VM, 11.0.24
    Branch HEAD
    Compiled by user runner on 2025-09-17T20:22:32Z
    Revision ed00d046951a7ecda6429accd3b9c5b2dc792b65
    Url https://github.com/apache/spark
    Type --help for more information.
    ```

# Contents & completion
## Contents
### (1) Spark Reading Data from File Systems
- (1) Read the local file `/home/hadoop/test.txt` from the Linux system in the spark-shell, and then count the number of lines in the file.
- (2) Read the HDFS system file `/user/hadoop/test.txt` in the spark-shell (if the file does not exist, create it first), and then count the number of lines in the file.
- (3) Read the local file `/home/hadoop/test.txt` from the Linux system in the spark-shell, and then run the APIs from the table below. Write the returned results.
    | Action API | Description |
    | - | - |
    | count() | Returns the number of elements in the dataset. |
    | collect() | Returns all elements in the dataset as an array. |
    | first() | Returns the first element in the dataset. |
    | take(n) | Returns the first n elements in the dataset as an array. |
    | reduce(func) | Aggregates the elements of the dataset using the specified binary function (func). |
    | foreach(func) | Applies the given function (func) to each element in the dataset. |

    | Transformation API | Description |
    | - | - |
    | filter(func) | Returns a new dataset formed by selecting elements that satisfy the given function (func). |
    | map(func) | Returns a new dataset formed by passing each element through the given function (func). |
    | flatMap(func) | Similar to map, but each input element can be mapped to zero or more output elements. |
    | groupByKey() | When applied to a dataset of (K, V) pairs, returns a dataset of (K, Iterable) pairs. |
    | reduceByKey(func) | When applied to a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given function (func). |

### (2) Additional Task: Write a Standalone Application for Data Deduplication
For two input files A and B, write a standalone Spark application to merge the two files and remove any duplicate content, resulting in a new file C. Below is an example of the input and output files for reference.

Input File A Sample:
```text
20170101    x
20170102    y
20170103    x
20170104    y
20170105    z
20170106    z
```

Input File B Sample:
```text
20170101    y
20170102    y
20170103    x
20170104    z
20170105    y
```

Output File C Sample (after merging and deduplication):
```text
20170101    x
20170101    y
20170102    y
20170103    x
20170104    y
20170104    z
20170105    y
20170105    z
20170106    z
```

## Completion
```scala
/*
2.
(1)
*/
// (1)
val local_file=sc.textFile("file:///home/hadoop/test.txt")
println("local file line count: "+local_file.count())
// (2)
val hdfs_file= sc.textFile("hdfs:///user/hadoop/test.txt")
println("hdfs file line count: "+hdfs_file.count())
// (3)
val file=sc.textFile("file:///home/hadoop/test.txt")
// Action API
println("count(): "+file.count())
println("collect(): "+file.collect().mkString(", "))
println("first(): "+file.first())
print("n = ")
val n=scala.io.StdIn.readInt()
println("take(n): "+file.take(n).mkString(", "))
println("reduce(func): No func specified")
println("foreach(func): No func specified")
// Transformation API
println("filter(func): No func specified")
println("map(func): No func specified")
println("flatMap(func): No func specified")
println("groupByKey(): No (K,V) specified")
println("reduceByKey(): No (K,V) specified")

/*
(2)
*/
print("Path to fileA: ")
val fileA=sc.textFile("file://"+new java.io.File(scala.io.StdIn.readLine()).getAbsolutePath)
print("Path to fileB: ")
val fileB=sc.textFile("file://"+new java.io.File(scala.io.StdIn.readLine()).getAbsolutePath)
print("Path to fileC: ")
val pathC="file://"+new java.io.File(scala.io.StdIn.readLine()).getAbsolutePath
fileA.union(fileB).distinct().sortBy(line => line).coalesce(1).saveAsTextFile(pathC)

```

# Problems
1. Java 11 was not competible with the latest Spark version, since Spark 4 requires Java 17 or above.
2. Can't see the input prompt when using scala.io.StdIn.readLine() in spark-shell.

# Solutions
1. Downgrade Spark to version 3.5.7 which is compatible with Java 11.
2. `:load <path/to/the/file>` to load the code from file in the spark-shell, but the input issue still exists.
