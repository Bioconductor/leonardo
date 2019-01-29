package org.broadinstitute.dsde.workbench.leonardo.service


import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.AutoFreezeConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.Retry
import cats.data.OptionT
import cats.implicits._



import scala.concurrent.{ExecutionContext, Future}

trait CommonLeoService extends LazyLogging with Retry{

  private[service] def calculateAutopauseThreshold(autopause: Option[Boolean], autopauseThreshold: Option[Int], autoFreezeConfig: AutoFreezeConfig): Int = {
    val AutoPauseOffValue = 0

    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        AutoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(AutoPauseOffValue, autopauseThreshold.get)
    }
  }

  private[service] def addDataprocWorkerRoleToServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail], googleIamDAO: GoogleIamDAO)(implicit executor: ExecutionContext): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamFuture: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.addIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamFuture
    } getOrElse Future.successful(())
  }

  private[service] def removeDataprocWorkerRoleFromServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail], googleIamDAO: GoogleIamDAO)(implicit executor: ExecutionContext): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamFuture: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.removeIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamFuture
    } getOrElse Future.successful(())
  }

  private[service] def removeServiceAccountKey(googleProject: GoogleProject, clusterName: ClusterName, serviceAccountOpt: Option[WorkbenchEmail], googleIamDAO: GoogleIamDAO, dbRef: DbReference)(implicit ec: ExecutionContext): Future[Unit] = {
    // Delete the service account key in Google, if present_
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(googleProject, clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](serviceAccountOpt)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(googleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }



  private[service] def template(raw: String, replacementMap: Map[String, String]): String = {
    replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  }

  private[service] def whenGoogle409(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => t.getStatusCode == 409
      case _ => false
    }
  }
}
