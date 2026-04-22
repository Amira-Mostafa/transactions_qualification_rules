version := "0.1.0"
scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "transactions_qualification_rules",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.4.7",
      "mysql" % "mysql-connector-java" % "8.0.33",
      "com.typesafe" % "config" % "1.4.2",
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
    )
  )

