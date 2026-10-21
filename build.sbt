name := "EcommerceAnalytics"
version := "1.0.0"
organization := "com.ecommerce"

// Spark 4.x est compile pour Scala 2.13
scalaVersion := "2.13.18"

val sparkVersion = "4.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  "com.typesafe"      % "config"     % "1.4.3",
  "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
)

// Spark est en "provided" pour le jar, mais on le remet dans le classpath pour sbt run
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// Spark a besoin de ces options avec Java 17+
val sparkJavaOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

run / fork := true
// sinon sbt affiche tout ce que Spark ecrit sur stderr avec le prefixe [error]
run / outputStrategy := Some(StdoutOutput)
run / javaOptions ++= sparkJavaOpts
Test / fork := true
Test / javaOptions ++= sparkJavaOpts
// une seule SparkSession a la fois pendant les tests
Test / parallelExecution := false

Compile / mainClass := Some("com.ecommerce.analytics.MainApp")

// JAR executable (fat jar sans Spark)
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "ecommerce-analytics.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "reference.conf"                     => MergeStrategy.concat
  case _                                    => MergeStrategy.first
}
