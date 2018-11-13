resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

lazy val commonSettings = Seq(
  organization := "pub.sha0w",
  version := "alpha-dev-0.1"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    javacOptions++=Seq("-source","1.8","-target","1.8"),
    name := "SeqN3CSVProcessor",
    scalaVersion := "2.11.8",
    scalaVersion in ThisBuild := "2.11.8",
    mainClass in Compile := Some("Main")
  ).enablePlugins()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.4.1",
//  "org.eclipse.rdf4j" % "rdf4j-rio-ntriples" % "2.4.1",
  "org.eclipse.rdf4j" % "rdf4j-rio-n3" % "2.4.1",
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
  //  "org.slf4j" % "slf4j-api" % "1.7.25",
//  "org.slf4j" % "slf4j-log4j12" % "1.7.25"
//  "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
)