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
    scalaVersion := "2.12.7",
    scalaVersion in ThisBuild := "2.12.7",
    mainClass in Compile := Some("etl.n3CSVDFRefactor")
  ).enablePlugins()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.eclipse.rdf4j" % "rdf4j-rio-api" % "2.4.1",
  "org.eclipse.rdf4j" % "rdf4j-rio-ntriples" % "2.4.1",
  "org.eclipse.rdf4j" % "rdf4j-rio-n3" % "2.4.1"
)