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
