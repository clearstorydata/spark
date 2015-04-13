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

package org.apache.spark.ui.jobs

import scala.xml.{Node, NodeSeq, Unparsed}

import java.util.Date
import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.jobs.UIData.JobUIData
import org.apache.spark.JobExecutionStatus

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val startTime: Option[Long] = parent.sc.map(_.startTime)
  private val listener = parent.listener

  private def applicationTimelineView(jobs: Seq[JobUIData], now: Long): Seq[Node] = {
    val jobEventJsonAsStrSeq = jobs.flatMap { jobUIData =>
      val jobId = jobUIData.jobId
      val status = jobUIData.status
      val submissionTimeOpt = jobUIData.startTime
      val completionTimeOpt = jobUIData.endTime

      if (status == JobExecutionStatus.UNKNOWN || submissionTimeOpt.isEmpty ||
        completionTimeOpt.isEmpty && status != JobExecutionStatus.RUNNING) {
        None
      }

      val submissionTime = submissionTimeOpt.get
      val completionTime = completionTimeOpt.getOrElse(now)
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
      }

      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content">Job ${jobId}</div>',
           |  'title': 'Job ${jobId}\\nStatus: ${status}\\n' +
           |    'Submission Time: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |    '${
                   if (status != JobExecutionStatus.RUNNING) {
                     s"""\\nCompletion Time: ${UIUtils.formatDate(new Date(completionTime))}"""
                   } else {
                     ""
                   }
                 }'
           |}
         """.stripMargin
      Option(jobEventJsonAsStr)
    }

    val executorEventJsonAsStrSeq =
      (listener.executorIdToAddedTime ++
        listener.executorIdToRemovedTimeAndReason).map { event =>
        val (executorId: String, status: String, time: Long, reason: Option[String]) =
          event match {
            case (executorId, (removedTime, reason)) =>
              (executorId, "removed", removedTime, Some(reason))
            case (executorId, addedTime) =>
              (executorId, "added", addedTime, None)
          }
        s"""
           |{
           |  'className': 'executor ${status}',
           |  'group': 'executors',
           |  'start': new Date(${time}),
           |  'content': '<div>Executor ${executorId} ${status}</div>',
           |  'title': '${if (status == "added") "Added" else "Removed"} ' +
           |    'at ${UIUtils.formatDate(new Date(time))}' +
           |    '${if (reason.isDefined) s"""\\nReason: ${reason.get}""" else ""}'
           |}
         """.stripMargin
      }

    val executorsLegend =
      <div class="legend-area"><svg width="200px" height="55px">
        <rect x="5px" y="5px" width="20px" height="15px"
          rx="2px" ry="2px" stroke="#97B0F8" fill="#D5DDF6"></rect>
        <text x="35px" y="17px">Executor Added</text>
        <rect x="5px" y="35px" width="20px" height="15px"
          rx="2px" ry="2px" stroke="#97B0F8" fill="#EBCA59"></rect>
        <text x="35px" y="47px">Executor Removed</text>
      </svg></div>.toString.filter(_ != '\n')

    val jobsLegend =
      <div class="legend-area"><svg width="200px" height="85px">
        <rect x="5px" y="5px" width="20px" height="15px"
          rx="2px" ry="2px" stroke="#97B0F8" fill="#D5DDF6"></rect>
        <text x="35px" y="17px">Succeeded Job</text>
        <rect x="5px" y="35px" width="20px" height="15px"
          rx="2px" ry="2px" stroke="#97B0F8" fill="#FF5475"></rect>
        <text x="35px" y="47px">Failed Job</text>
        <rect x="5px" y="65px" width="20px" height="15px"
          rx="2px" ry="2px" stroke="#97B0F8" fill="#FDFFCA"></rect>
        <text x="35px" y="77px">Running Job</text>
      </svg></div>.toString.filter(_ != '\n')

    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${executorsLegend}',
          |  },
          |  {
          |    'id': 'jobs',
          |    'content': '<div>Jobs</div>${jobsLegend}',
          |  }
          |]
        """.stripMargin

    val eventArrayAsStr =
      (jobEventJsonAsStrSeq ++ executorEventJsonAsStrSeq).mkString("[", ",", "]")

    <div class="control-panel">
      <div id="application-timeline-zoom-lock">
        <input type="checkbox" checked="checked"></input>
        <span>Zoom Lock</span>
      </div>
    </div> ++
    <div id="application-timeline"></div> ++
    <script type="text/javascript">
      {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}, ${eventArrayAsStr});")}
    </script>
  }

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)

    val columns: Seq[Node] = {
      <th>{if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"}</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val lastStageInfo = Option(job.stageIds)
        .filter(_.nonEmpty)
        .flatMap { ids => listener.stageIdToInfo.get(ids.max) }
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }

      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
      val duration: Option[Long] = {
        job.startTime.map { start =>
          val end = job.endTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.startTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val detailUrl =
        "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), job.jobId)
      <tr>
        <td sorttable_customkey={job.jobId.toString}>
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td>
          <span class="description-input" title={lastStageDescription}>{lastStageDescription}</span>
          <a href={detailUrl}>{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.startTime.getOrElse(-1).toString}>
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
        <td class="stage-progress-cell">
          {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
          {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
          {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
        </td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
           failed = job.numFailedTasks, skipped = job.numSkippedTasks,
           total = job.numTasks - job.numSkippedTasks)}
        </td>
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq
      val now = System.currentTimeMillis

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(_.startTime.getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(_.endTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(_.endTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (startTime.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                {UIUtils.formatDuration(now - startTime.get)}
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveJobs) {
                <li>
                  <a href="#active"><strong>Active Jobs:</strong></a>
                  {activeJobs.size}
                </li>
              }
            }
            {
              if (shouldShowCompletedJobs) {
                <li>
                  <a href="#completed"><strong>Completed Jobs:</strong></a>
                  {completedJobs.size}
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  {failedJobs.size}
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      content ++= <h4>Events on Application Timeline</h4> ++
        applicationTimelineView(activeJobs ++ completedJobs ++ failedJobs, now)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
          activeJobsTable
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobs.size})</h4> ++
          completedJobsTable
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
          failedJobsTable
      }

      val helpText = """A job is triggered by an action, like "count()" or "saveAsTextFile()".""" +
        " Click on a job's title to see information about the stages of tasks associated with" +
        " the job."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}
