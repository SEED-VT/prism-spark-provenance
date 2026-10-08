ThisBuild / organization := "edu.vt.prism"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.1.2"

// JDK 17+ module opens required by Spark 4 at runtime
val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val prism = (project in file("."))
  .settings(
    name := "prism",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      // Provided by the Spark distribution at run time.
      "org.roaringbitmap" % "RoaringBitmap" % "1.2.1" % Provided,
      "it.unimi.dsi" % "fastutil" % "8.5.15",
      "com.google.guava" % "guava" % "33.4.0-jre" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    javacOptions ++= Seq("--release", "17"),
    // `sbt doc` writes the API documentation to target/scala-2.13/api. The root page
    // comes from rootdoc.txt, and -private documents the internal capture engine too.
    Compile / doc / scalacOptions ++= Seq(
      "-doc-title", "Prism: field-level data provenance for Spark 4",
      "-doc-version", version.value,
      "-doc-root-content", ((Compile / scalaSource).value / "rootdoc.txt").getAbsolutePath,
      "-groups",
      "-private",
      "-no-link-warnings"
    ),
    Test / fork := true,
    Test / javaOptions ++= sparkJavaOptions,
    // The local-cluster tests launch executor JVMs through Spark's launcher, which needs
    // a Spark distribution at SPARK_HOME and the Scala version that Spark's own scripts
    // would export.
    Test / envVars ++= Map(
      "SPARK_HOME" -> sys.env.getOrElse("SPARK_HOME", ""),
      "SPARK_SCALA_VERSION" -> "2.13"
    )
  )
