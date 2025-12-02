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

# Problems

# Solutions
