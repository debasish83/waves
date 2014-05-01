import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.0",
  organization := "io.swave",
  homepage := Some(new URL("http://swave.io")),
  description := "A reactive streams implementation in Scala",
  startYear := Some(2014),
  licenses := Seq("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  javacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-source", "1.6",
    "-target", "1.6",
    "-Xlint:unchecked",
    "-Xlint:deprecation"),
  scalacOptions ++= List(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-language:_",
    "-target:jvm-1.6",
    "-Xlog-reflective-calls"),
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " })

val formattingSettings = scalariformSettings ++ Seq(
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(PreserveDanglingCloseParenthesis, true))

val publishingSettings = Seq(
  publishMavenStyle := true,
  useGpg := true,
  publishTo <<= version { v: String =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else                             Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  pomIncludeRepository := { _ => false },
  pomExtra :=
    <scm>
      <url>git@github.com:sirthias/swave.git</url>
      <connection>scm:git:git@github.com:sirthias/swave.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sirthias</id>
        <name>Mathias Doenitz</name>
      </developer>
    </developers>)

/////////////////////// DEPENDENCIES /////////////////////////

val `akka-actor`           = "com.typesafe.akka"    %% "akka-actor"  % "2.3.2"  % "compile"
val `reactive-streams`     = "org.reactivestreams"       % "reactive-streams-spi"         % "0.3"
val `reactive-streams-tck` = "org.reactivestreams"      % "reactive-streams-tck"         % "0.3"              % "test"
val specs2                 = "org.specs2" %% "specs2-core" % "2.3.11" % "test"

/////////////////////// PROJECTS /////////////////////////

lazy val examples = project
  .dependsOn(swave)
  .settings(commonSettings: _*)
  .settings(cappiSettings: _*)
  .settings(
    publishTo := None
  )

lazy val swave = project
  .settings(commonSettings: _*)
  .settings(formattingSettings: _*)
  .settings(publishingSettings: _*)
  .settings(
    libraryDependencies ++= Seq(`akka-actor`, `reactive-streams`, `reactive-streams-tck`, specs2)
  )