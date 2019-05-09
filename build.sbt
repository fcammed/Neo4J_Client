name := "Neo4J_Client"

version := "0.1"

scalaVersion := "2.12.8"

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

mainClass in assembly := Some("HelloWorld")

libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.5.1"
// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-java8-compat
libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.8"
libraryDependencies += "io.reactiverse" % "reactive-pg-client" % "0.11.2"

libraryDependencies += "org.jooq" % "jooq" % "3.11.11"
libraryDependencies += "org.jooq" % "jooq-meta" % "3.11.11"
libraryDependencies += "org.jooq" % "jooq-codegen" % "3.11.11"

lazy val root = (project in file(".")).enablePlugins()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}