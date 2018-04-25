import AssemblyKeys._

assemblySettings

name := "CRS2-Hbase2Hive"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.10.2" %  "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.0-cdh5.10.2" %  "provided",
  "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.10.2"  %  "provided",
  "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.10.2"  %  "provided",
  "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.10.2"  %  "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.6"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)