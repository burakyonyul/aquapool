name := "axe"

version := "0.1"

scalaVersion := "2.12.6"

val jenaVersion = "2.11.0"

val akkaVersion = "2.6.20"

val gJsonVersion = "2.8.2"

val jsonVersion = "2.6.7"

val elastic4sVersion = "8.5.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.jena" % "jena-arq" % jenaVersion,
  "org.apache.jena" % "jena-core" % jenaVersion,
  "com.typesafe.play" %% "play-json" % jsonVersion,
  "net.debasishg" %% "redisclient" % "3.7",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "io.kamon" % "sigar-loader" % "1.6.6-rev002",
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "3.3.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided",
  "org.postgresql" % "postgresql" % "42.5.1",
  "com.influxdb" %% "influxdb-client-scala" % "6.6.0",
  "com.github.nscala-time" %% "nscala-time" % "2.32.0",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "8.5.3",
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test"
).map(_ exclude("org.slf4j", "*"))
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.6"
libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.1"





//libraryDependencies ++= Seq(
//  "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.103",
//  "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.103" % Test
//)

//resolvers += Resolver.sonatypeRepo("snapshots")

enablePlugins(JavaAppPackaging)