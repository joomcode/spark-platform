# Joom Spark Platform

This repository is the home for foundational Spark tools used by the analytics Platform at Joom.

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
