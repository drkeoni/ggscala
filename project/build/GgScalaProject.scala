import sbt._

class GgScalaProject( info: ProjectInfo ) extends DefaultProject(info)
{
  // ScalaTest
  val scalatest = "org.scalatest" %% "scalatest" % "1.4.1"
}
