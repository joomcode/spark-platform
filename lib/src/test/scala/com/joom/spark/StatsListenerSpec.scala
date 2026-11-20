package com.joom.spark

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.joom.spark.monitoring.{ApplicationSummary, StageSummary, StatsReportingSparkListener}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.junit.JUnitRunner

import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class StatsListenerSpec extends FlatSpec with Matchers {
  private case class CapturedRequest(path: String, authorization: String, body: String)

  "StatsReportingSparkListener" should "report stage metadata to the configured base URL" in {
    implicit val stageCodec: JsonValueCodec[List[StageSummary]] = JsonCodecMaker.make
    implicit val appCodec: JsonValueCodec[List[ApplicationSummary]] = JsonCodecMaker.make
    val requests = new ConcurrentLinkedQueue[CapturedRequest]()
    val finishedReceived = new CountDownLatch(1)
    val outputReceived = new CountDownLatch(1)
    val prefix = "/custom/sparkperformance"
    val propertyName = "spark.platform.test.stage"
    val propertyValue = "listener-compatibility"
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

    // This test must never use a production endpoint or a developer's token.
    server.createContext(prefix, new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val source = scala.io.Source.fromInputStream(exchange.getRequestBody, "UTF-8")
        val body = try source.mkString finally source.close()
        val path = exchange.getRequestURI.getPath
        requests.add(CapturedRequest(path, exchange.getRequestHeaders.getFirst("Authorization"), body))
        try {
          if (path == prefix + "/apps" && readFromString[List[ApplicationSummary]](body).exists(_.finished)) {
            finishedReceived.countDown()
          }
          if (path == prefix + "/stages" && readFromString[List[StageSummary]](body).exists(_.outputGB > 0)) {
            outputReceived.countDown()
          }
        } finally {
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
        }
      }
    })
    server.start()

    try {
      val spark = SparkSession.builder()
        .master("local[1]")
        .appName("spark-platform-listener-test")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      var listener: StatsReportingSparkListener = null
      try {
        listener = new StatsReportingSparkListener(
          spark.sparkContext.getConf,
          "test-api-key",
          Set(propertyName),
          s"http://127.0.0.1:${server.getAddress.getPort}$prefix/")
        spark.sparkContext.addSparkListener(listener)
        spark.sparkContext.setLocalProperty(propertyName, propertyValue)
        val output = Files.createTempDirectory("spark-platform-listener-").resolve("rows")
        spark.range(0, 16).write.parquet(output.toUri.toString)
        spark.stop()

        finishedReceived.await(5, TimeUnit.SECONDS) shouldBe true
        outputReceived.await(5, TimeUnit.SECONDS) shouldBe true
        val captured = requests.asScala.toVector
        captured.nonEmpty shouldBe true
        captured.forall(_.path.startsWith(prefix + "/")) shouldBe true
        captured.forall(_.authorization == "Bearer test-api-key") shouldBe true
        val stages = captured.filter(_.path == prefix + "/stages")
          .flatMap(request => readFromString[List[StageSummary]](request.body))
        stages.exists(stage => stage.name.exists(_.nonEmpty) && stage.outputGB > 0 &&
          stage.properties.get(propertyName).contains(propertyValue)) shouldBe true
        captured.filter(_.path == prefix + "/apps")
          .flatMap(request => readFromString[List[ApplicationSummary]](request.body))
          .exists(_.finished) shouldBe true
      } finally {
        spark.stop()
        if (listener != null) listener.completeQueue()
      }
    } finally {
      server.stop(0)
    }
  }

  "sigma" should "work" in {
    import StatsReportingSparkListener.sigma

    sigma(Seq.empty[Double]) should be(0.0)
    sigma(Seq(7)) should be(0.0)
    // Mean is 2, Variance is (1 + 0 + 1)/3 = 0.666, SD = 0.81
    StatsReportingSparkListener.sigma(Seq(1, 2, 3)) should be(0.81 +- 0.1)
  }
}
