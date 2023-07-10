name := "axe"

version := "0.1"

scalaVersion := "2.12.6"

val jenaVersion = "2.11.0"

val akkaVersion = "2.6.20"

val jsonVersion = "2.6.7"

val elastic4sVersion = "8.5.3"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
  "org.apache.jena" % "jena-arq" % jenaVersion,
  "org.apache.jena" % "jena-core" % jenaVersion,
  "com.typesafe.play" %% "play-json" % jsonVersion,
  "net.debasishg" %% "redisclient" % "3.7",
  "io.kamon" % "sigar-loader" % "1.6.6-rev002",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.5.1",
  "com.influxdb" %% "influxdb-client-scala" % "6.6.0",
  "com.github.nscala-time" %% "nscala-time" % "2.32.0",
  "org.elasticsearch" %% "elasticsearch-spark-30" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "org.scala-lang" % "scala-reflect" % "2.12.6",
  "com.zaxxer" % "HikariCP" % "5.0.1",
  "io.aeron" % "aeron-driver" % "1.40.0",
  "io.aeron" % "aeron-client" % "1.40.0"
).map(_ exclude("org.slf4j", "*"))
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.11"

//libraryDependencies ++= Seq(
//  "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.103",
//  "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % "0.103" % Test
//)

//resolvers += Resolver.sonatypeRepo("snapshots")

enablePlugins(JavaAppPackaging)