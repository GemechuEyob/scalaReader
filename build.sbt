name := "scalaReader"
version := "0.1"
scalaVersion := "2.12.13"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
// https://mvnrepository.com/artifact/com.ibm.stocator/stocator
libraryDependencies += "com.ibm.stocator" % "stocator" % "1.1.3"
dependencyOverrides += "com.google.guava" % "guava" % "15.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "com.ibm.db2" % "jcc" % "11.5.5.0"
