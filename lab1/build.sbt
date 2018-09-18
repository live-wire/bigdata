ThisBuild / scalaVersion := "2.11.12"

lazy val example = (project in file("."))
  .settings(
    name := "GDelt",
    fork in run := true,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" 
  )