/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.status

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.api.v1.{StageData, StageStatus}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore._

class ElementTrackingStorePerfSuite extends SparkFunSuite {
  val useLevelDB = false

  import config._

  case class ElementData(stages: Seq[StageDataWrapper], summaries: Seq[ExecutorStageSummaryWrapper])

  def setup[T](test: ElementTrackingStore => T): T = {
    val store =
      if (useLevelDB) {
        KVUtils.open(Utils.createTempDir(), getClass.getName)
      } else {
        new InMemoryStore()
      }
    val tracking = new ElementTrackingStore(store, new SparkConf()
      .set(ASYNC_TRACKING_ENABLED, false))

    test(tracking)
  }

  def initializeData(): ElementData = {
    val count = 1000
    val strings = (0 to 4).map { _.toString }

    val stages =
      (0 until count).map { index =>
        new StageDataWrapper(
          new StageData(
            StageStatus.COMPLETE,
            index,
            1,
            numTasks = 1,
            numActiveTasks = 0,
            numCompleteTasks = 1,
            numFailedTasks = 0,
            numKilledTasks = 0,
            numCompletedIndices = 0,

            executorRunTime = 1000,
            executorCpuTime = 1000,
            submissionTime = None,
            firstTaskLaunchedTime = None,
            completionTime = None,
            failureReason = None,

            inputBytes = 0,
            inputRecords = 0,
            outputBytes = 0,
            outputRecords = 0,
            shuffleReadBytes = 0,
            shuffleReadRecords = 0,
            shuffleWriteBytes = 0,
            shuffleWriteRecords = 0,
            memoryBytesSpilled = 0,
            diskBytesSpilled = 0,

            name = "Test",
            description = None,
            details = "Some details",
            schedulingPool = "june",

            rddIds = Nil,
            accumulatorUpdates = Nil,
            tasks = None,
            executorSummary = None,
            killedTasksSummary = Map.empty), jobIds = null, locality = null)
      }

      val summaries =
        (0 until count).flatMap { index =>
          (0 to (index % 5)).map { summaryIndex =>
            new ExecutorStageSummaryWrapper(index, 1, strings(summaryIndex), null)
          }
      }

    ElementData(stages, summaries)
  }

  def populate(tracking: ElementTrackingStore, data: ElementData): Unit = {
    data.stages.foreach { stage =>
      tracking.write(stage)
    }

    data.summaries.foreach { summary =>
      tracking.write(summary)
    }
  }

  def perfTest(tracking: ElementTrackingStore)(f: => Unit): Unit = {
    var totalTime = 0L
    var totalUsedMemory = 0L
    val data = initializeData()
    // warmup!
    (0 to 3).foreach { _ =>
      populate(tracking, data)
      f
    }

    (0 to 9).foreach { _ =>
      populate(tracking, data)
      System.gc()
      val freeMemory = Runtime.getRuntime.freeMemory()
      val startTime = System.nanoTime()
      f
      totalTime += (System.nanoTime() - startTime)
      totalUsedMemory = freeMemory - Runtime.getRuntime.freeMemory()
      // scalastyle:off println
      println(s"Took ${(System.nanoTime() - startTime)/1000}")
    }
    println(s"Took an average ${totalTime / 10 / 1000}us, and used ${totalUsedMemory/10} bytes")
    // scalastyle:on println
  }

  test("originalSpeed n^2logn") {
    setup { tracking =>
      perfTest(tracking) {
        val stages = tracking.view(classOf[StageDataWrapper]).asScala

        stages.foreach { s =>
          val key = Array(s.info.stageId, s.info.attemptId)

          tracking.delete(s.getClass, key)

          tracking.view(classOf[ExecutorStageSummaryWrapper])
            .index("stage")
            .first(key)
            .last(key)
            .asScala
            .foreach { e =>
              tracking.delete(e.getClass, e.id)
            }
        }
      }
    }
  }

  test("removeAllByKeys") {
    setup { tracking =>
      perfTest(tracking) {
        val stages = tracking.view(classOf[StageDataWrapper]).asScala

        assert(tracking.count(classOf[ExecutorStageSummaryWrapper]) > 0)
        val stageKeys = stages.map { s =>
          val key = Array(s.info.stageId, s.info.attemptId)

          tracking.delete(s.getClass, key)

          key
        }

        tracking.removeAllByKeys(classOf[ExecutorStageSummaryWrapper], "stage", stageKeys)

        assert(tracking.count(classOf[ExecutorStageSummaryWrapper]) == 0)
      }
    }
  }

  test("removeAllByKeys staged") {
    setup { tracking =>
      perfTest(tracking) {
        val allStages = tracking.view(classOf[StageDataWrapper]).asScala

        assert(tracking.count(classOf[ExecutorStageSummaryWrapper]) > 0)
        allStages.grouped(allStages.size / 30).foreach { stages =>
          val stageKeys = stages.map { s =>
            val key = Array(s.info.stageId, s.info.attemptId)

            tracking.delete(s.getClass, key)

            key
          }

          tracking.removeAllByKeys(classOf[ExecutorStageSummaryWrapper], "stage", stageKeys)
        }

        assert(tracking.count(classOf[ExecutorStageSummaryWrapper]) == 0)
      }
    }
  }
}
