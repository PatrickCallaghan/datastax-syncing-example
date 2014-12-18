import AssemblyKeys._

name := "sparksync"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.6" 
)

libraryDependencies ++= Seq(
    ("org.apache.spark"%%"spark-core"%"1.0.0").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.mail").
     exclude("org.eclipse.jetty.orbit", "javax.activation").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)
	
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)	
	
assemblySettings

resolvers += "AkkaRepository" at "http://repo.akka.io/releases/"
