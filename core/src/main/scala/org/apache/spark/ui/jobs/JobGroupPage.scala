package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.jobs.UIData.JobUIData
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

/** Page showing list of jobs under a job group id */
private[ui] class JobGroupPage(parent: JobsTab) extends AllJobsPage(parent) {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing parameter id")

      val jobGroupId = parameterId
      val groupToJobsTable = listener.jobGroupToJobIds.get(jobGroupId)
      if (groupToJobsTable.isEmpty) {
        val content =
          <div id="no-info">
            <p>No information to display for jobGroup
              {jobGroupId}
            </p>
          </div>
        return UIUtils.headerSparkPage(
          s"Details for JobGroup $jobGroupId", content, parent)
      }

      val jobsInGroup = listener.jobIdToData

      val activeJobsInGroup = mutable.Buffer[JobUIData]()
      val completedJobsInGroup = mutable.Buffer[JobUIData]()
      val failedJobsInGroup = mutable.Buffer[JobUIData]()
      groupToJobsTable.get.foreach { jobId =>
        val job = jobsInGroup.get(jobId)
        job.get.status match {
          case JobExecutionStatus.RUNNING => activeJobsInGroup ++= job
          case JobExecutionStatus.SUCCEEDED => completedJobsInGroup ++= job
          case JobExecutionStatus.FAILED => failedJobsInGroup ++= job
        }
      }

      val activeJobsTable =
        jobsTable(activeJobsInGroup.sortBy(_.submissionTime.getOrElse((-1L))).reverse)
      val completedJobsTable =
        jobsTable(completedJobsInGroup.sortBy(_.completionTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobsInGroup.sortBy(_.completionTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobsInGroup.nonEmpty
      val shouldShowCompletedJobs = completedJobsInGroup.nonEmpty
      val shouldShowFailedJobs = failedJobsInGroup.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (shouldShowActiveJobs) {
            <li>
              <a href="#active">
                <strong>Active Jobs:</strong>
              </a>{activeJobsInGroup.size}
            </li>
          }}{if (shouldShowCompletedJobs) {
            <li id="completed-summary">
              <a href="#completed">
                <strong>Completed Jobs:</strong>
              </a>{completedJobsInGroup.size}
            </li>
          }}{if (shouldShowFailedJobs) {
            <li>
              <a href="#failed">
                <strong>Failed Jobs:</strong>
              </a>{listener.numFailedJobs}
            </li>
          }}
          </ul>
        </div>

      var content = summary
      val executorListener = parent.executorListener
      content ++= makeTimeline(activeJobsInGroup ++ completedJobsInGroup ++ failedJobsInGroup,
        executorListener.executorIdToData, listener.startTime)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs (
          {activeJobsInGroup.size}
          )</h4> ++
          activeJobsTable
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs (
          {completedJobsInGroup.size}
          )</h4> ++
          completedJobsTable
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id="failed">Failed Jobs (
          {failedJobsInGroup.size}
          )</h4> ++
          failedJobsTable
      }

      val helpText =
        """A job is triggered by an action, like count() or saveAsTextFile().""" +
          " Click on a job to see information about the stages of tasks inside it."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }

  }

}
