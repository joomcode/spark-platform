package com.joom.spark.monitoring

case class ApplicationSummary(
                               ts: Long,
                               appId: String,
                               appName: String,
                               configuration: Map[String, String],
                               finished: Boolean = false,
                               duration: Option[Long] = None,
                               executorMetrics: Option[Map[String, Seq[ExecutorMetric]]] = None,
                             )

case class StageSummary(
                         ts: Long,
                         appId: String,
                         stageId: Int,
                         attemptNumber: Int,
                         succeeded: Boolean,
                         failureReason: Option[String],
                         tasksCount: Int,
                         taskFailures: Int,
                         executorRunTimeSum: Double, // In seconds
                         executorRunTimeMax: Double,
                         executorRunTime75Percentile: Double,
                         executorRunTime95Percentile: Double,
                         executorRunTimeMedian: Double,
                         executorRunTimeSigma: Double,
                         failedTasksTimeSum: Double,
                         shuffleTotalReadGB: Double,
                         shuffleRemoteReadGB: Double,
                         shuffleRemoteReadMaxGB: Double,
                         shuffleRemoteRead95PercentileGB: Double,
                         shuffleRemoteReadMedianGB: Double,
                         memorySpillGB: Double,
                         diskSpillGB: Double,
                         inputGB: Double,
                         outputGB: Double,
                         shuffleWriteGB: Double,
                         peakExecutionMemoryGB: Double,
                         properties: Map[String, String],
                         endTs: Long,
                         name: Option[String],
                       )

case class ExecutorMetric(
                           execId: String,
                           value: Long,
                         )

case class ExecutorAdded(
                          ts: Long,
                          appId: String,
                          executorId: String,
                          executorHost: Option[String],
                          totalCores: Option[Int],
                        )

case class ExecutorRemoved(
                            ts: Long,
                            appId: String,
                            executorId: String,
                            reason: String,
                          )

case class AbruptShutdown(
                         ts: Long,
                         appId: String
                         )
