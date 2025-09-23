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
