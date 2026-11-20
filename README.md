# Joom Spark Platform

This repository is the home for foundational Spark tools used by the analytics Platform at Joom.

## Installation

The packages for Spark 3.2.2 with both Scala 2.12 and 2.13 are available from the maven central repository.
For Spark 4.2, use the internal Joom Maven release described below; these
installation examples refer to the existing Spark 3 releases.

When using sbt:

```
libraryDependencies += "com.joom.spark" % "spark-platform_2.12" % "0.1.2"
```

When using Gradle:

```
implementation group: 'com.joom.spark', name: 'spark-platform_2.12', version: '0.1.2'
```

## Building for different Spark versions

The same `lib` project and shared Scala sources support the existing Spark 3
matrix and Spark 4.2. Select the dependency versions with `scala.version` and
`spark.version`; the artifact name contains only the Scala binary version.
Internal reflection adapters bridge Spark API differences, as with row encoders.

| Target | Scala | Build/test Java | Default publication version |
| --- | --- | --- | --- |
| Existing Spark 3.2–3.5 matrix | Existing 2.12 / 2.13 versions | Java 8 bytecode; existing test-JDK selection | `0.4.11` |
| Spark 4.2.1 | 2.13.18 | Java 17 | `0.4.11-joom-2` |

`java.test.versions` still overrides additional test JDKs. Its default remains
`8` for Spark 3 and is `17` for Spark 4. `testJdk17` on Spark 4 delegates to the
normal Java 17 test task, so the suite runs once. The existing Spark 3 CI matrix
is retained; Spark 4.2.1 has a separate Scala 2.13.18 / Java 17 matrix entry.

With `JAVA_HOME` pointing to a JDK 17 installation:

```sh
./gradlew --no-daemon -Dorg.gradle.vfs.watch=false --console=plain \
  :lib:build :lib:sourcesJar :lib:javadocJar :lib:generatePomFileForMavenPublication \
  :lib:generateMetadataFileForMavenPublication \
  -Pscala.version=2.13.18 -Pspark.version=4.2.1 -Pjava.test.versions=17 \
  -Ppublication.version=0.4.11-joom-2 -Ppublication.signing.enabled=false
```

The runtime tests exercise both custom planning strategies and use a loopback
HTTP server with a fake key for listener reporting. They verify configurable
`baseUrl`, stage name/output metrics and application completion without a cloud
token or requests to the production API.

Join regression tests compare against Spark's own SQL equality in ANSI and
non-ANSI modes, including mixed key types and collations, and check that null
keys remain distributed during a shuffled join. Deduplication uses SQL ordering
for key equality, including signed zero, NaN and Spark 4 collations.

## Publishing the Spark 4 build to Joom Maven

The intended new internal coordinate is:

```text
com.joom.spark:spark-platform_2.13:0.4.11-joom-2
```

Spark 3 artifacts keep their existing versions. Use the existing
`publication.version` override for subsequent releases, and allocate a new
version rather than overwriting an existing release. Spark 3 and Spark 4 builds
with Scala 2.13 have the same artifact ID and must use distinct release versions.

Configure `JFROG_USER` and `JFROG_PASSWORD` through the usual secret mechanism
(or the existing `jfrogMavenUser` / `jfrogMavenPassword` Gradle properties).
After the build and review, publish only to the existing `JoomMaven` repository:

```sh
./gradlew --no-daemon -Dorg.gradle.vfs.watch=false --console=plain \
  :lib:build :lib:publishMavenPublicationToJoomMavenRepository \
  -Pscala.version=2.13.18 -Pspark.version=4.2.1 -Pjava.test.versions=17 \
  -Ppublication.version=0.4.11-joom-2 -Ppublication.signing.enabled=false
```

That task uploads the JAR, sources, documentation and metadata to
`https://jfrog.joom.it/artifactory/joom-maven`. Use this repository-specific task;
no Maven Central/Sonatype credentials or public release tag are needed. The new
Spark 4 CI lane does not match the existing Spark-3.5-only public publication gate.

Once published and resolved from Joom Maven, the datasci Spark 4 platform can
constrain the coordinate above. Do not change the existing Spark 3 constraint.

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
