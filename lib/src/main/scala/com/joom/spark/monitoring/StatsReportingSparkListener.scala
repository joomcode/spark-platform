package com.joom.spark.monitoring

import java.time.{Duration, Instant}
import org.apache.spark.{SparkConf, Success, TaskEndReason}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.slf4j.LoggerFactory

import java.io.IOException
import scala.collection.mutable

class StatsReportingSparkListener(sparkConf: SparkConf, apiKey: String) extends SparkListener {
  import StatsReportingSparkListener._
  private val appId = sparkConf.getAppId
  private val appName = sparkConf.get(AppNameKey)
  private val httpClient = new OkHttpClient()
  private val maxSendAttempts = 3
  private val log = LoggerFactory.getLogger(this.getClass)

  case class StageFullId(stageId: Int, attemptNumber: Int)
  case class StageState(startTime: Instant = Instant.now(),
                        var completed: Boolean = false,
                        var sent: Boolean = false,
                        var startedTaskCount: Int = 0,
                        var failureReason: Option[String] = None)
  private val tasksPerStage = mutable.Map[StageFullId, mutable.ArrayBuffer[(TaskMetrics, TaskEndReason)]]()
  private val stageState = mutable.Map[StageFullId, StageState]()
  private val appStart: Instant = Instant.now()
  private val executorMetrics = mutable.Map[String, mutable.Map[String, Long]]() // Map[metricName, Map[execId, Value]]
  private val addedExecutorIds = mutable.Set[String]()

  implicit val codec: JsonValueCodec[ApplicationSummary] = JsonCodecMaker.make
  send("apps", ApplicationSummary(appStart.toEpochMilli, appId, appName, sparkConf.getAll.toMap))

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    stageState.filter { case (_, state) => !state.sent && state.completed }.foreach { case (stageFullId, _) =>
      sendStageSummaryIfReady(stageFullId, force = true)
    }

    send("apps", ApplicationSummary(appStart.toEpochMilli, appId, appName, sparkConf.getAll.toMap,
      finished = true,
      duration = Option(Duration.between(appStart, Instant.now()).toMillis / 1000L),
      executorMetrics = Some(executorMetrics.map { case (k, v) =>
        (k, v.map { case (execId, value) => ExecutorMetric(execId, value)}.toSeq)
      }.toMap)
    ))
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageFullId = StageFullId(stageSubmitted.stageInfo.stageId, stageSubmitted.stageInfo.attemptNumber)
    stageState.getOrElseUpdate(stageFullId, StageState())
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageFullId = StageFullId(stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber())
    val state = stageState.getOrElseUpdate(stageFullId, StageState())
    state.failureReason = stageCompleted.stageInfo.failureReason
    state.completed = true
    sendStageSummaryIfReady(stageFullId)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val stageFullId = StageFullId(taskStart.stageId, taskStart.stageAttemptId)
    stageState.getOrElseUpdate(stageFullId, StageState()).startedTaskCount += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageFullId = StageFullId(taskEnd.stageId, taskEnd.stageAttemptId)
    tasksPerStage.getOrElseUpdate(stageFullId, mutable.ArrayBuffer[(TaskMetrics, TaskEndReason)]())
      .append((taskEnd.taskMetrics, taskEnd.reason))
    sendStageSummaryIfReady(stageFullId)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    implicit val codec: JsonValueCodec[ExecutorRemoved] = JsonCodecMaker.make
    send("executor-removed", ExecutorRemoved(
      executorRemoved.time,
      appId,
      executorRemoved.executorId,
      executorRemoved.reason
    ))
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(
      executorAdded.time,
      appId,
      executorAdded.executorId,
      Some(executorAdded.executorInfo.executorHost),
      Some(executorAdded.executorInfo.totalCores)
    )
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    if (!addedExecutorIds.contains(executorMetricsUpdate.execId) && executorMetricsUpdate.execId != "driver") {
      // SparkListener can only be added after SparkSession is created. By this time, some executors may already
      // have been created. In that case, onExecutorAdded will not be called for them. So we check if the information
      // about adding this executor has been sent, and if not, do it. We assume that the creation time of this
      // executor is close to the creation time of this SparkListener.
      addExecutor(appStart.toEpochMilli, appId, executorMetricsUpdate.execId)
    }

    executorMetricsUpdate.executorUpdates.foreach { case ((_, _), metrics) =>
      Seq("OnHeapStorageMemory", "DirectPoolMemory").foreach { metricName =>
        val newValue = metrics.getMetricValue(metricName)
        val metricMap = executorMetrics.getOrElseUpdate(s"${metricName}Peak", mutable.Map[String, Long]())
        val oldValue = metricMap.getOrElse(executorMetricsUpdate.execId, 0L)
        if (newValue > oldValue)
          metricMap.update(executorMetricsUpdate.execId, newValue)
      }
    }
  }

  private def addExecutor(ts: Long, appId: String, executorId: String, executorHost: Option[String] = None,
                          totalCores: Option[Int] = None): Unit = {
    implicit val codec: JsonValueCodec[ExecutorAdded] = JsonCodecMaker.make
    send("executor-added", ExecutorAdded(ts, appId, executorId, executorHost, totalCores))
    addedExecutorIds.add(executorId)
  }

  private def sendStageSummaryIfReady(stageFullId: StageFullId, force: Boolean = false): Unit = {
    stageState.get(stageFullId).foreach { state =>
      val tasks = tasksPerStage.getOrElse(stageFullId, mutable.ArrayBuffer[(TaskMetrics, TaskEndReason)]())

      if (!state.sent && (force || (state.completed && tasks.size == state.startedTaskCount))) {
        val success = state.failureReason.isEmpty
        val failureReason = state.failureReason
        val startTime = state.startTime
        val summary = summarizeStage(appId, stageFullId.stageId, stageFullId.attemptNumber, success, failureReason,
          startTime, tasks.toSeq)

        implicit val codec: JsonValueCodec[StageSummary] = JsonCodecMaker.make
        send("stages", summary.get)
        state.sent = true

        // TaskMetrics has a lot of data, including references to SQLMetric and a bunch of accumulators
        // needed for those. In long running processes, such as Zeppelin, these can consume a lot of
        // memory. Clean them up.
        tasksPerStage.remove(stageFullId)
      }
    }
  }

  private def send[Record](kind: String, record: Record)(implicit codec: JsonValueCodec[Record]): Unit = {
    val json = writeToString(record)
    val body = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json)
    val request = new Request.Builder()
      .url(BaseUrl + kind)
      .addHeader("Authorization", "Bearer " + apiKey)
      .post(body)
      .build()

    def executeRequest(request: Request, attemptNum: Int): Boolean = {
      try {
        using(httpClient.newCall(request).execute()) { response =>
          if (!response.isSuccessful) {
            log.warn(s"Failed to log Spark event (attempt #$attemptNum): kind - $kind, json - $json, " +
              s"responseCode - ${response.code()}, responseMessage - ${response.message()}")
          }
          response.isSuccessful
        }
      } catch {
        case e: IOException =>
          log.warn(s"Failed to log Spark event (attempt #$attemptNum): kind - $kind, json - $json", e)
          false
      }
    }

    (0 until maxSendAttempts).exists { attemptNum =>
      if (attemptNum > 0)
        Thread.sleep(attemptNum * 2000)
      executeRequest(request, attemptNum)
    }
  }
}

object StatsReportingSparkListener {
  private val GiB = math.pow(1024, 3)
  private val AppNameKey = "spark.app.name"
  private val BaseUrl = "https://api.cloud.joom.ai/v1/sparkperformance/"

  private def sigma(values: Seq[Double]) = {
    if (values.isEmpty) 0
    else {
      val mean = values.sum / values.length
      val mos = values.fold(0.0)((r, n) => r + n * n) / values.length
      val som = mean * mean
      val variance = mos - som
      math.sqrt(variance)
    }
  }

  private def median(values: Seq[Double]) = {
    if (values.isEmpty) 0
    else {
      val sorted = values.sorted
      sorted(values.length / 2)
    }
  }

  private def percentile(values: Seq[Double], percentile: Int) = {
    if (values.isEmpty) 0
    else {
      val sorted = values.sorted
      val index = Math.ceil(percentile / 100.0 * sorted.size).toInt
      sorted(index - 1)
    }
  }

  private def max(values: Seq[Double]) = {
    if (values.isEmpty) 0
    else values.max
  }

  private def summarizeStage(appId: String, stageId: Int, attemptNumber: Int, succeeded: Boolean,
                             failureReason: Option[String], startTime: Instant,
                             rawTaskMetrics: Seq[(TaskMetrics, TaskEndReason)]): Option[StageSummary] = {
    val taskMetrics = rawTaskMetrics.map(_._1)
      .filter(_ != null) // For failed tasks, there will be 'null' TaskMetrics instances.
    val runTimes = taskMetrics.map(_.executorRunTime.toDouble / 1000.0)
    val shuffleRemoteReadGb = taskMetrics.map(_.shuffleReadMetrics.remoteBytesRead / GiB)
    val failedTaskMetrics = rawTaskMetrics.filter(_._2 != Success).map(_._1)

    Some(StageSummary(
      ts = startTime.toEpochMilli,
      appId = appId,
      stageId = stageId,
      attemptNumber = attemptNumber,
      succeeded = succeeded,
      failureReason = failureReason,
      tasksCount = runTimes.length,
      taskFailures = failedTaskMetrics.length,
      executorRunTimeSum = runTimes.sum,
      executorRunTimeMax = max(runTimes),
      executorRunTime75Percentile = percentile(runTimes, 75),
      executorRunTime95Percentile = percentile(runTimes, 95),
      executorRunTimeMedian = median(runTimes),
      executorRunTimeSigma = sigma(runTimes),
      failedTasksTimeSum = failedTaskMetrics.filter(_ != null).map(_.executorRunTime.toDouble / 1000.0).sum,
      shuffleTotalReadGB = taskMetrics.map(_.shuffleReadMetrics.totalBytesRead).sum / GiB,
      shuffleRemoteReadGB = shuffleRemoteReadGb.sum,
      shuffleRemoteReadMaxGB = max(shuffleRemoteReadGb),
      shuffleRemoteRead95PercentileGB = percentile(shuffleRemoteReadGb, 95),
      shuffleRemoteReadMedianGB = median(shuffleRemoteReadGb),
      memorySpillGB = taskMetrics.map(_.memoryBytesSpilled.toDouble).sum / GiB,
      diskSpillGB = taskMetrics.map(_.diskBytesSpilled).sum / GiB,
      inputGB = taskMetrics.map(_.inputMetrics.bytesRead).sum / GiB,
      shuffleWriteGB = taskMetrics.map(_.shuffleWriteMetrics.bytesWritten).sum / GiB,
      peakExecutionMemoryGB = taskMetrics.map(_.peakExecutionMemory).sum / GiB,
    ))
  }
}
