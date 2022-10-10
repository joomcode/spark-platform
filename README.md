# Joom Spark Platform

This repository is the home for foundational Spark tools used by the analytics Platform at Joom.

## Installation

The packages for Spark 3.2.2 with both Scala 2.12 and 2.13 are available from the maven central repository.

When using sbt:

```
libraryDependencies += "com.joom.spark" % "spark-platform_2.12" % "0.1.2"
```

When using Gradle:

```
implementation group: 'com.joom.spark', name: 'spark-platform_2.12', version: '0.1.2'
```

## Explicit repartitionioning

The `ExplicitRepartition` class is used to explicitly control which partition a given row
should be in - and is immune to hash collisions. The usage is as simple as 
```
val rdf = df
    .withColumn("desired_partition", ...your expression...)
    .explicitRepartition(8, $"desired_partition")
```

Please see the [testcase](https://github.com/joomcode/spark-platform/blob/main/lib/src/test/scala/com/joom/spark/ExplicitRepartitionSpec.scala) for
the complete example, and for details and motivation, please see the companion blog post:
[Spark Partitioning: Full Control](https://medium.com/@vladimir.prus/spark-partitioning-full-control-3c72cea2d74d)
