import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-leadscoring"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "org.apache.predictionio"    %% "apache-predictionio-core"          % pioVersion.value % "provided",
  "org.apache.spark"           %% "spark-core"                        % "1.3.0"          % "provided",
  "org.apache.spark"           %% "spark-mllib"                       % "1.3.0"          % "provided")
