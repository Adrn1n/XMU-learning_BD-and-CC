mkdir input
cp $HADOOP_HOME/etc/hadoop/*.xml input/
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar grep input/ output/ 'dfs[a-z.]+'
cat output/*

