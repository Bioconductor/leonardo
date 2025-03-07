package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object MockRStudioDAO extends RStudioDAO {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] =
    Future.successful(true)
}
