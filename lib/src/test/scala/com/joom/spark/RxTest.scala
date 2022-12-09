package com.joom.spark

import io.reactivex.rxjava3.core.BackpressureStrategy
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

@RunWith(classOf[JUnitRunner])
class RxTest extends FlatSpec with Matchers {
  "main thread" should "not block" in {
    val processed = new AtomicInteger(0)
    val dropped = new AtomicInteger(0)

    val subject = PublishSubject.create[String]()
    var completed = false
    val lock = new ReentrantLock()
    val condition = lock.newCondition()
    subject
      // Create Flowable, so that we can actually limit the buffer size, see below
      // We can either use DROP strategy, or use MISSING with custom reporting
      .toFlowable(BackpressureStrategy.MISSING)
      .onBackpressureDrop(s => {
        println(s"Dropping ${s}")
        dropped.incrementAndGet()
      })
      // Move data to processing thread, using a fixed buffer size. Without
      // explicit buffer size, it will be unbounded, and we really don't want
      // unbounded memory consumption in case our service is down.
      // Note that this semantics works only for Flowable, for Observable
      // the 'bufferSize' parameter is actually specifying buffer chunk size,
      // while the buffer will still be unbounded.
      .observeOn(Schedulers.io(), false, 5)
      .subscribe((s: String) => {
        println(s"Processing ${s}")
        processed.incrementAndGet()
        Thread.sleep(1000)
      }, (t: Throwable) => {}, () => {
        println("Completed")
        lock.lock()
        completed = true
        condition.signalAll()
        lock.unlock()
      })

    for (r <- 1 to 20) {
      subject.onNext(r.toString)
    }
    println("Posted everything to subject")
    Thread.sleep(7*1000)

    subject.onComplete()

    var nanos = TimeUnit.SECONDS.toNanos(1)
    lock.lock()
    try {
      while (!completed && nanos > 0) {
        nanos = condition.awaitNanos(nanos)
      }
    } finally {
      lock.unlock()
    }
    println(s"Completed wait for completion with ${nanos}ns to spare")

    processed.get() shouldBe 5
    dropped.get() shouldBe 15
  }
}
