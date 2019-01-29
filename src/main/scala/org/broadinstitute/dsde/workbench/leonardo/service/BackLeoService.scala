package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKey, generateUniqueBucketName}
import org.broadinstitute.dsde.workbench.util.Retry
import cats.implicits._


import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Failure

class BackLeoService (protected val dataprocConfig: DataprocConfig,
                      protected val clusterFilesConfig: ClusterFilesConfig,
                      protected val clusterResourcesConfig: ClusterResourcesConfig,
                      protected val clusterDefaultsConfig: ClusterDefaultsConfig,
                      protected val proxyConfig: ProxyConfig,
                      protected val swaggerConfig: SwaggerConfig,
                      protected val autoFreezeConfig: AutoFreezeConfig,
                      protected val gdDAO: GoogleDataprocDAO,
                      protected val googleComputeDAO: GoogleComputeDAO,
                      protected val googleIamDAO: GoogleIamDAO,
                      protected val leoGoogleStorageDAO: GoogleStorageDAO,
                      protected val petGoogleStorageDAO: String => GoogleStorageDAO,
                      protected val dbRef: DbReference,
                      protected val authProvider: LeoAuthProvider,
                      protected val serviceAccountProvider: ServiceAccountProvider,
                      protected val whitelist: Set[String],
                      protected val bucketHelper: BucketHelper,
                      protected val contentSecurityPolicy: String)
                     (implicit val executionContext: ExecutionContext,
                      implicit override val system: ActorSystem) extends LazyLogging with Retry with CommonLeoService {

  private lazy val firewallRule = FirewallRule(
    name = FirewallRuleName(dataprocConfig.firewallRuleName),
    protocol = FirewallRuleProtocol(proxyConfig.jupyterProtocol),
    ports = List(FirewallRulePort(proxyConfig.jupyterPort.toString)),
    network = dataprocConfig.vpcNetwork.map(VPCNetworkName),
    targetTags = List(NetworkTag(dataprocConfig.networkTag)))



  def completeClusterCreation(userEmail: WorkbenchEmail,
                                      cluster: Cluster,
                                      clusterRequest: ClusterRequest): Future[Cluster] = {
    logger.info(s"Submitting to Google the request to create cluster '${cluster.clusterName}' " +
      s"on Google project '${cluster.googleProject}'...")

    for {
      (googleCluster, initBucket, serviceAccountKey) <- createGoogleCluster(userEmail, cluster, clusterRequest)

      // We overwrite googleCluster.id with the DB-assigned one that was obtained when we first
      // inserted the record into the DB prior to completing the createCluster request
      googleClusterWithUpdatedId = googleCluster.copy(id = cluster.id)

      _ <- dbRef.inTransaction {
        _.clusterQuery
          .updateAsyncClusterCreationFields(
            Option(GcsPath(initBucket, GcsObjectName(""))), serviceAccountKey, googleClusterWithUpdatedId)
      }
    } yield googleClusterWithUpdatedId
  }


  private[service] def createGoogleCluster(userEmail: WorkbenchEmail,
                                           cluster: Cluster,
                                           clusterRequest: ClusterRequest)
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    createGoogleCluster(userEmail, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest, cluster.clusterImages)
  }

  /* Creates a cluster in the given google project:
     - Add a firewall rule to the user's google project if it doesn't exist, so we can access the cluster
     - Create the initialization bucket for the cluster in the leo google project
     - Upload all the necessary initialization files to the bucket
     - Create the cluster in the google project */
  private[service] def createGoogleCluster(userEmail: WorkbenchEmail,
                                           serviceAccountInfo: ServiceAccountInfo,
                                           googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           clusterRequest: ClusterRequest,
                                           clusterImages: Set[ClusterImage])
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-"+clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-"+clusterName.value)

    val googleFuture = for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- googleComputeDAO.updateFirewallRule(googleProject, firewallRule)

      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- generateServiceAccountKey(googleProject, serviceAccountInfo.notebookServiceAccount)

      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- addDataprocWorkerRoleToServiceAccount(googleProject, serviceAccountInfo.clusterServiceAccount, googleIamDAO)

      // Create the bucket in leo's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the bucket at initialization time.
      initBucket <- bucketHelper.createInitBucket(googleProject, initBucketName, serviceAccountInfo)
      _ <- initializeBucketObjects(userEmail, googleProject, clusterName, initBucket, clusterRequest, serviceAccountKeyOpt, contentSecurityPolicy, clusterImages)

      // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
      stagingBucket <- bucketHelper.createStagingBucket(userEmail, googleProject, stagingBucketName, serviceAccountInfo)

      // Create the cluster
      machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
      initScript = GcsPath(initBucket, GcsObjectName(clusterResourcesConfig.initActionsScript.value))
      autopauseThreshold = calculateAutopauseThreshold(clusterRequest.autopause, clusterRequest.autopauseThreshold, autoFreezeConfig)
      clusterScopes = clusterRequest.scopes.getOrElse(dataprocConfig.defaultScopes)
      credentialsFileName = serviceAccountInfo.notebookServiceAccount.map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")
      operation <- gdDAO.createCluster(googleProject, clusterName, machineConfig, initScript,
        serviceAccountInfo.clusterServiceAccount, credentialsFileName, stagingBucket, clusterScopes)
      cluster = Cluster.create(clusterRequest, userEmail, clusterName, googleProject, serviceAccountInfo,
        machineConfig, dataprocConfig.clusterUrlBase, autopauseThreshold, clusterScopes, Option(operation), Option(stagingBucket), clusterImages)
    } yield (cluster, initBucket, serviceAccountKeyOpt)

    // If anything fails, we need to clean up Google resources that might have been created
    googleFuture.andThen { case Failure(t) =>
      // Don't wait for this future
      cleanUpGoogleResourcesOnError(t, googleProject, clusterName, initBucketName, serviceAccountInfo)
    }
  }

  private[service] def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Option[ServiceAccountKey]] = {
    serviceAccountOpt.traverse { serviceAccountEmail =>
      googleIamDAO.createServiceAccountKey(googleProject, serviceAccountEmail)
    }
  }



  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(userEmail: WorkbenchEmail,
                                               googleProject: GoogleProject,
                                               clusterName: ClusterName,
                                               initBucketName: GcsBucketName,
                                               clusterRequest: ClusterRequest,
                                               serviceAccountKey: Option[ServiceAccountKey],
                                               contentSecurityPolicy: String,
                                               clusterImages: Set[ClusterImage]): Future[Unit] = {

    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val clusterInit = ClusterInitValues(googleProject, clusterName, initBucketName, clusterRequest, dataprocConfig,
      clusterFilesConfig, clusterResourcesConfig, proxyConfig, serviceAccountKey, userEmail, contentSecurityPolicy, clusterImages)
    val replacements: Map[String, String] = clusterInit.toMap

    // Raw files to upload to the bucket, no additional processing needed.
    val filesToUpload = List(
      clusterFilesConfig.jupyterServerCrt,
      clusterFilesConfig.jupyterServerKey,
      clusterFilesConfig.jupyterRootCaPem)

    // Raw resources to upload to the bucket, no additional processing needed.
    // Note: initActionsScript and jupyterGoogleSignInJs are not included
    // because they are post-processed by templating logic.
    val resourcesToUpload = List(
      clusterResourcesConfig.clusterDockerCompose,
      clusterResourcesConfig.jupyterProxySiteConf,
      clusterResourcesConfig.jupyterCustomJs)

    // Uploads the service account private key to the init bucket, if defined.
    // This is a no-op if createClusterAsPetServiceAccount is true.
    val uploadPrivateKeyFuture: Future[Unit] = serviceAccountKey.flatMap(_.privateKeyData.decode).map { k =>
      leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename), k, "text/plain")
    } getOrElse(Future.successful(()))

    // Fill in templated resources with the given replacements
    val initScriptContent = templateResource(clusterResourcesConfig.initActionsScript, replacements)
    val googleSignInJsContent = templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)
    val jupyterNotebookConfigContent = templateResource(clusterResourcesConfig.jupyterNotebookConfigUri, replacements)

    for {
      // Upload the init script to the bucket
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.initActionsScript.value), initScriptContent, "text/plain")

      // Upload the googleSignInJs file to the bucket
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterGoogleSignInJs.value), googleSignInJsContent, "text/plain")

      // Update the jupytyer notebook config file
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.value), jupyterNotebookConfigContent, "text/plain")

      // Upload raw files (like certs) to the bucket
      _ <- Future.traverse(filesToUpload)(file => leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(file.getName), file, "text/plain"))

      // Upload raw resources (like cluster-docker-compose.yml, site.conf) to the bucket
      _ <- Future.traverse(resourcesToUpload) { resource =>
        val content = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
        leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(resource.value), content, "text/plain")
      }

      // Update the private key json, if defined
      _ <- uploadPrivateKeyFuture
    } yield ()
  }

  private[service] def cleanUpGoogleResourcesOnError(throwable: Throwable, googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[Unit] = {
    logger.error(s"Cluster creation failed in Google for $googleProject / ${clusterName.value}. Cleaning up resources in Google...")

    // Clean up resources in Google

    val deleteInitBucketFuture = leoGoogleStorageDAO.deleteBucket(initBucketName, recurse = true) map { _ =>
      logger.info(s"Successfully deleted init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}", e)
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteClusterFuture = gdDAO.deleteCluster(googleProject, clusterName) map { _ =>
      logger.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete cluster ${googleProject.value} / ${clusterName.value}", e)
    }

    val deleteServiceAccountKeyFuture = removeServiceAccountKey(googleProject, clusterName, serviceAccountInfo.notebookServiceAccount, googleIamDAO, dbRef) map { _ =>
      logger.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    } recover { case e =>
      logger.error(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}", e)
    }

    Future.sequence(Seq(deleteInitBucketFuture, deleteClusterFuture, deleteServiceAccountKeyFuture)).void
  }

  private[service] def templateResource(resource: ClusterResource, replacementMap: Map[String, String]): String = {
    val raw = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
    template(raw, replacementMap)
  }

}
