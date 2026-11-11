name := "spark-lab4"
version := "0.1.0"
scalaVersion := "2.13.16"
Compile / mainClass := Some("IpEventCount")

// Spark доступен в runtime classpath sbt run; поэтому зависимости без Provided.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.1",
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "com.univocity" % "univocity-parsers" % "2.9.1"
)

// JVM приложения отделена от JVM sbt; Spark локально стартует внутри приложения.
fork := true
outputStrategy := Some(StdoutOutput)

// Доступ к модулям JDK 17/21, необходимый Spark и его сериализаторам.
javaOptions ++= Seq(
  "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
  "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
  "jdk.internal.ref", "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"
).map(pkg => s"--add-opens=java.base/$pkg=ALL-UNNAMED") ++ Seq(
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
  "--add-modules=jdk.incubator.vector",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)
