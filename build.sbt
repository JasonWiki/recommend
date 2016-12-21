import AssemblyKeys._

// 打开 assembly 插件功能
assemblySettings

// 配置 assembly 插件所有使用的 JAR
jarName in assembly := "recommend-2.0.jar"

// 项目名称
name := "recommend-2.0"

// 组织名称
organization := "com.angejia.dw.recommend"

// 项目版本号
version := "2.0"

// scala 版本
scalaVersion := "2.11.8"

// Eclipse 支持
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// 非托管资源目录
unmanagedResourceDirectories in Compile += { baseDirectory.value / "src/main/resources" }

// 相关依赖
libraryDependencies ++= Seq(
    // scala-library
    "org.scala-lang" % "scala-library" % "2.11.8",

    // hadoop 依赖
    "org.apache.hadoop" % "hadoop-common" % "2.6.0",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0",

    // Spark 依赖 : spark-core_2.11(spark 所属 scala 版本号) 2.0.2(spark 版本号)
    "org.apache.spark" % "spark-core_2.11" % "2.0.2",
    "org.apache.spark" % "spark-streaming_2.11" % "2.0.2",
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.2"
    //"org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.2"
        exclude("org.apache.avro","*")
        exclude("org.slf4j","*"),
    "org.apache.spark" % "spark-mllib_2.11" % "2.0.2",
    // spark sql
    "org.apache.spark" % "spark-sql_2.11" % "2.0.2",
    "org.apache.spark" % "spark-hive_2.11" % "2.0.2",
    //"org.apache.avro"  % "avro" % "1.7.4",
    //"org.apache.avro"  % "avro-ipc" % "1.7.4" excludeAll(excludeNetty),

    // hive 相关 JDBC
    "org.apache.hive" % "hive-common" % "1.1.0",
    //"org.apache.hive" % "hive-exec" % "1.1.0",
    "org.apache.hive" % "hive-jdbc" % "1.1.0", 
    "org.apache.hive" % "hive-cli" % "1.1.0",
    //"org.spark-project.hive" % "hive-beeline" % "1.2.1.spark2",

    // jblas 线性代数库,求向量点积
    "org.jblas" % "jblas" % "1.2.4",

    // Kafka 依赖 
    "org.apache.kafka" % "kafka-log4j-appender" % "0.10.1.0" % "provided",
    "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
        exclude("javax.jms", "jms")
        exclude("com.sun.jdmk", "jmxtools") 
        exclude("com.sun.jmx", "jmxri"),

    // Hbase 依赖
    //"org.apache.hbase" % "hbase" % "1.0.0",
    "org.apache.hbase" % "hbase-common" % "1.0.0",
    "org.apache.hbase" % "hbase-client" % "1.0.0",
    "org.apache.hbase" % "hbase-server" % "1.0.0",
    //"org.apache.hbase" % "hbase-protocol" % "1.0.0",
    //"org.apache.htrace" % "htrace-core" % "3.1.0-incubating",

    // Mysql 依赖
    "mysql" % "mysql-connector-java" % "5.1.38",

    // ES 客户端
    //"org.elasticsearch" % "elasticsearch" % "2.3.4",
    // 原始 elasticsearch 依赖因为 guava 包会产生冲突 , HBase 使用的是 12.0,  ES 使用的是 19.0 的版本
    // 解决方法: http://blog.csdn.net/sunshine920103/article/details/51659936
    //"com.angejia.dw.elasticsearch" % "dw_elasticsearch" % "1.0",

    // play Json 包, 版本太高会冲突
    "com.typesafe.play" % "play-json_2.11" % "2.3.9",
    // spray Json 包
    "io.spray" % "spray-json_2.11" % "1.3.2",
    // smart Json 包
    "net.minidev" % "json-smart" % "2.2.1",

    // java Json 包
    "com.googlecode.json-simple" % "json-simple" % "1.1.1",

    // ORM 框架 Hibernate
    //"org.hibernate" % "hibernate-core" % "5.2.1.Final",
    //"org.hibernate.javax.persistence" % "hibernate-jpa-2.0-api" % "1.0.1.Final",
    //"commons-logging" % "commons-logging" % "1.2",
    //"commons-collections" % "commons-collections" % "3.2.2",
    //"cglib" % "cglib" % "3.2.4",
    //"dom4j" % "dom4j" % "1.6.1",

    // 其他
    "net.sf.jopt-simple" % "jopt-simple" % "4.9" % "provided",
    "joda-time" % "joda-time" % "2.9.2" % "provided",
    "commons-codec" % "commons-codec" % "1.10",
    "log4j" % "log4j" % "1.2.9",
    "com.github.scopt" %% "scopt" % "3.5.0",

    // 单元测试框架
    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)


// 强制默认合并
mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}


// 配置远程资源
resolvers ++= Seq(

      // HTTPS is unavailable for Maven Central
      "Maven Repository"     at "http://repo.maven.apache.org/maven2",
      "Apache Repository"    at "https://repository.apache.org/content/repositories/releases",
      "JBoss Repository"     at "https://repository.jboss.org/nexus/content/repositories/releases/",
      "Cloudera Repository"  at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Elaticsearch Repository" at "https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch",
      "MQTT Repository"      at "https://repo.eclipse.org/content/repositories/paho-releases/",

      // For Sonatype publishing
      // "sonatype-snapshots"   at "https://oss.sonatype.org/content/repositories/snapshots",
      // "sonatype-staging"     at "https://oss.sonatype.org/service/local/staging/deploy/maven2/",
      // also check the local Maven repository ~/.m2  "/usr/local/maven/repository"

      //本地 mavan 仓库地址
      "Local Maven Repository" at "file:///usr/local/maven/repository",
      Resolver.mavenLocal 
)

