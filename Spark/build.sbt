ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"




libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"