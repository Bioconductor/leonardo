package org.broadinstitute.dsde.workbench.leonardo
package dao.google

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.OptionT
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{
  Cluster => DataprocCluster,
  ClusterConfig => DataprocClusterConfig,
  Operation => DataprocOperation,
  ClusterStatus => _,
  _
}
import com.google.api.services.oauth2.Oauth2
import org.broadinstitute.dsde.workbench.google.AbstractHttpGoogleDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.service.{AuthenticationError, DataprocDisabledException}
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchException, WorkbenchUserId}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class HttpGoogleDataprocDAO(
  appName: String,
  googleCredentialMode: GoogleCredentialMode,
  override val workbenchMetricBaseName: String,
  networkTag: NetworkTag,
  defaultRegion: String,
  zoneOpt: Option[String]
)(implicit override val system: ActorSystem, override val executionContext: ExecutionContext)
    extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName)
    with GoogleDataprocDAO {

  implicit override val service: GoogleInstrumentedService = GoogleInstrumentedService.Dataproc

  override val scopes: Seq[String] = Seq(ComputeScopes.CLOUD_PLATFORM)

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName)
      .build()
  }

  // TODO move out of this DAO
  private lazy val oauth2 =
    new Oauth2.Builder(httpTransport, jsonFactory, null)
      .setApplicationName(appName)
      .build()

  override def createCluster(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             createClusterConfig: CreateClusterConfig): Future[Operation] = {
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.value)
      .setConfig(getClusterConfig(createClusterConfig))

    val request = dataproc.projects().regions().clusters().create(googleProject.value, defaultRegion, cluster)

    retryWithRecover(retryPredicates: _*) { () =>
      executeGoogleRequest(request)
    } {
      case e: GoogleJsonResponseException
          if e.getStatusCode == 403 &&
            (e.getDetails.getErrors.get(0).getReason == "accessNotConfigured") =>
        throw DataprocDisabledException(e.getMessage)
    }.map { op =>
        Operation(OperationName(op.getName), getOperationUUID(op))
      }
      .handleGoogleException(googleProject, Some(clusterName.value))

  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.value, defaultRegion, clusterName.value)
    retryWithRecover(retryPredicates: _*) { () =>
      executeGoogleRequest(request)
      ()
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => ()
      case e: GoogleJsonResponseException
          if e.getStatusCode == StatusCodes.BadRequest.intValue &&
            e.getDetails.getMessage.contains("pending delete") =>
        ()
    }.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      status <- OptionT.pure[Future](
        Try(ClusterStatus.withNameInsensitive(cluster.getStatus.getState)).toOption.getOrElse(ClusterStatus.Unknown)
      )
    } yield status

    transformed.value.map(_.getOrElse(ClusterStatus.Deleted)).handleGoogleException(googleProject, clusterName)
  }

  override def listClusters(googleProject: GoogleProject): Future[List[UUID]] = {
    val request = dataproc.projects().regions().clusters().list(googleProject.value, defaultRegion)
    // Use OptionT to handle nulls in the Google response
    val transformed = for {
      result <- OptionT.liftF(retry(retryPredicates: _*)(() => executeGoogleRequest(request)))
      googleClusters <- OptionT.fromOption[Future](Option(result.getClusters))
    } yield {
      googleClusters.asScala.toList.map(c => UUID.fromString(c.getClusterUuid))
    }
    transformed.value.map(_.getOrElse(List.empty)).handleGoogleException(googleProject)
  }

  override def getClusterMasterInstance(googleProject: GoogleProject,
                                        clusterName: ClusterName): Future[Option[InstanceKey]] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      masterInstanceName <- OptionT.fromOption[Future] { getMasterInstanceName(cluster) }
      masterInstanceZone <- OptionT.fromOption[Future] { getZone(cluster) }
    } yield InstanceKey(googleProject, masterInstanceZone, masterInstanceName)

    transformed.value.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterInstances(googleProject: GoogleProject,
                                   clusterName: ClusterName): Future[Map[DataprocRole, Set[InstanceKey]]] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      instanceNames <- OptionT.fromOption[Future] { getAllInstanceNames(cluster) }
      clusterZone <- OptionT.fromOption[Future] { getZone(cluster) }
    } yield {
      instanceNames.mapValues(_.map(name => InstanceKey(googleProject, clusterZone, name)))
    }

    transformed.value.map(_.getOrElse(Map.empty)).handleGoogleException(googleProject, clusterName)
  }

  override def getClusterStagingBucket(googleProject: GoogleProject,
                                       clusterName: ClusterName): Future[Option[GcsBucketName]] = {
    // If an expression might be null, need to use `OptionT.fromOption(Option(expr))`.
    // `OptionT.pure(expr)` throws a NPE!
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      config <- OptionT.fromOption[Future](Option(cluster.getConfig))
      bucket <- OptionT.fromOption[Future](Option(config.getConfigBucket))
    } yield GcsBucketName(bucket)

    transformed.value.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterErrorDetails(operationName: Option[OperationName]): Future[Option[ClusterErrorDetails]] = {
    val errorOpt: OptionT[Future, ClusterErrorDetails] = for {
      operationName <- OptionT.fromOption[Future](operationName)
      operation <- OptionT(getOperation(operationName)) if operation.getDone
      error <- OptionT.fromOption[Future] { Option(operation.getError) }
      code <- OptionT.fromOption[Future] { Option(error.getCode) }
    } yield ClusterErrorDetails(code, Option(error.getMessage))

    errorOpt.value.handleGoogleException(GoogleProject(""), operationName.map(_.value))
  }

  override def resizeCluster(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             numWorkers: Option[Int] = None,
                             numPreemptibles: Option[Int] = None): Future[Unit] = {
    val workerMask = "config.worker_config.num_instances"
    val preemptibleMask = "config.secondary_worker_config.num_instances"

    val updateAndMask = (numWorkers, numPreemptibles) match {
      case (Some(nw), Some(np)) =>
        val mask = List(Some(workerMask), Some(preemptibleMask)).flatten.mkString(",")
        val update = new DataprocCluster().setConfig(
          new DataprocClusterConfig()
            .setWorkerConfig(new InstanceGroupConfig().setNumInstances(nw))
            .setSecondaryWorkerConfig(new InstanceGroupConfig().setNumInstances(np))
        )
        Some((update, mask))

      case (Some(nw), None) =>
        val mask = workerMask
        val update = new DataprocCluster().setConfig(
          new DataprocClusterConfig()
            .setWorkerConfig(new InstanceGroupConfig().setNumInstances(nw))
        )
        Some((update, mask))

      case (None, Some(np)) =>
        val mask = preemptibleMask
        val update = new DataprocCluster().setConfig(
          new DataprocClusterConfig()
            .setSecondaryWorkerConfig(new InstanceGroupConfig().setNumInstances(np))
        )
        Some((update, mask))

      case (None, None) =>
        None
    }

    updateAndMask match {
      case Some((update, mask)) =>
        val request = dataproc
          .projects()
          .regions()
          .clusters()
          .patch(googleProject.value, defaultRegion, clusterName.value, update)
          .setUpdateMask(mask)
        retry(retryPredicates: _*)(() => executeGoogleRequest(request)).void
      case None => Future.successful(())
    }
  }

  override def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)] = {
    val request = oauth2.tokeninfo().setAccessToken(accessToken)
    retry(retryPredicates: _*)(() => executeGoogleRequest(request)).map { tokenInfo =>
      (UserInfo(OAuth2BearerToken(accessToken),
                WorkbenchUserId(tokenInfo.getUserId),
                WorkbenchEmail(tokenInfo.getEmail),
                tokenInfo.getExpiresIn.toInt),
       Instant.now().plusSeconds(tokenInfo.getExpiresIn.toInt))
    } recover {
      case e: GoogleJsonResponseException =>
        val msg = s"Call to Google OAuth API failed. Status: ${e.getStatusCode}. Message: ${e.getDetails.getMessage}"
        logger.error(msg, e)
        throw new WorkbenchException(msg, e)
      // Google throws IllegalArgumentException when passed an invalid token. Handle this case and rethrow a 401.
      case _: IllegalArgumentException =>
        throw AuthenticationError()
    }
  }

  private def getClusterConfig(config: CreateClusterConfig): DataprocClusterConfig = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    // applicable to all instances in the cluster.
    // Set the network tag, network, and subnet. This allows the created GCE instances to be exposed by Leo's firewall rule.
    val gceClusterConfig = {
      val baseConfig = new GceClusterConfig()
        .setTags(List(networkTag.value).asJava)

      config.clusterVPCSettings match {
        case Some(Right(subnet)) =>
          baseConfig.setSubnetworkUri(subnet.value)
        case Some(Left(network)) =>
          baseConfig.setNetworkUri(network.value)
        case _ =>
          baseConfig
      }
    }

    // Set the cluster service account, if present.
    // This is the service account passed to the create cluster API call.
    config.clusterServiceAccount.foreach { serviceAccountEmail =>
      gceClusterConfig
        .setServiceAccount(serviceAccountEmail.value)
        .setServiceAccountScopes(config.clusterScopes.toList.asJava)
    }

    // Create a NodeInitializationAction, which specifies the executable to run on a node.
    // This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
    val initActions = config.initScripts.map { script =>
      //null means no timeout. Otherwise, the default is 10 minutes. We provide timeouts in the cluster monitor.
      new NodeInitializationAction().setExecutableFile(script.toUri).setExecutionTimeout(null)
    }

    // Create a config for the master node, if properties are not specified in request, use defaults
    val masterConfig = new InstanceGroupConfig()
      .setMachineTypeUri(config.machineConfig.masterMachineType.get)
      .setDiskConfig(new DiskConfig().setBootDiskSizeGb(config.machineConfig.masterDiskSize.get))

    // If a custom dataproc image is specified, set it in the InstanceGroupConfig.
    // This overrides the imageVersion set in SoftwareConfig.
    config.dataprocCustomImage.foreach { customImage =>
      masterConfig.setImageUri(customImage)
    }

    // Set the zone, if specified. If not specified, Dataproc will pick a zone within the configured region.
    zoneOpt.foreach { zone =>
      gceClusterConfig.setZoneUri(zone)
    }

    // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the InstanceGroupConfig
    createClusterConfig(config)
      .setGceClusterConfig(gceClusterConfig)
      .setInitializationActions(initActions.asJava)
      .setMasterConfig(masterConfig)
      .setConfigBucket(config.stagingBucket.value)
  }

  // Expects a Machine Config with master configs defined for a 0 worker cluster and both master and worker
  // configs defined for 2 or more workers.
  private def createClusterConfig(createClusterConfig: CreateClusterConfig): DataprocClusterConfig = {

    val swConfig: SoftwareConfig = getSoftwareConfig(createClusterConfig)

    // If the number of workers is zero, make a Single Node cluster, else make a Standard one
    if (createClusterConfig.machineConfig.numberOfWorkers.get == 0) {
      new DataprocClusterConfig().setSoftwareConfig(swConfig)
    } else // Standard, multi node cluster
      getMultiNodeClusterConfig(createClusterConfig).setSoftwareConfig(swConfig)
  }

  private def getSoftwareConfig(createClusterConfig: CreateClusterConfig) = {
    val authProps: Map[String, String] = createClusterConfig.credentialsFileName match {
      case None =>
        // If we're not using a notebook service account, no need to set Hadoop properties since
        // the SA credentials are on the metadata server.
        Map.empty

      case Some(fileName) =>
        // If we are using a notebook service account, set the necessary Hadoop properties
        // to specify the location of the notebook service account key file.
        Map(
          "core:google.cloud.auth.service.account.enable" -> "true",
          "core:google.cloud.auth.service.account.json.keyfile" -> fileName
        )
    }

    val dataprocProps: Map[String, String] = if (createClusterConfig.machineConfig.numberOfWorkers.get == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    } else
      Map.empty

    val yarnProps = Map(
      // Helps with debugging
      "yarn:yarn.log-aggregation-enable" -> "true",
      // Dataproc 1.1 sets this too high (5586m) which limits the number of Spark jobs that can be run at one time.
      // This has been reduced drastically in Dataproc 1.2. See:
      // https://stackoverflow.com/questions/41185599/spark-default-settings-on-dataproc-especially-spark-yarn-am-memory
      // GAWB-3080 is open for upgrading to Dataproc 1.2, at which point this line can be removed.
      "spark:spark.yarn.am.memory" -> "640m"
    )

    val swConfig = new SoftwareConfig()
      .setProperties((authProps ++ dataprocProps ++ yarnProps ++ createClusterConfig.properties).asJava)

    if (createClusterConfig.dataprocCustomImage.isEmpty) {
      swConfig.setImageVersion("1.2-deb9")
    }

    swConfig
  }

  private def getMultiNodeClusterConfig(createClusterConfig: CreateClusterConfig): DataprocClusterConfig = {
    // Set the configs of the non-preemptible, primary worker nodes
    val clusterConfigWithWorkerConfigs =
      new DataprocClusterConfig().setWorkerConfig(getPrimaryWorkerConfig(createClusterConfig))

    // If the number of preemptible workers is greater than 0, set a secondary worker config
    if (createClusterConfig.machineConfig.numberOfPreemptibleWorkers.get > 0) {
      val preemptibleWorkerConfig = new InstanceGroupConfig()
        .setIsPreemptible(true)
        .setNumInstances(createClusterConfig.machineConfig.numberOfPreemptibleWorkers.get)

      // If a custom dataproc image is specified, set it in the InstanceGroupConfig.
      // This overrides the imageVersion set in SoftwareConfig.
      createClusterConfig.dataprocCustomImage.foreach { customImage =>
        preemptibleWorkerConfig.setImageUri(customImage)
      }

      clusterConfigWithWorkerConfigs.setSecondaryWorkerConfig(preemptibleWorkerConfig)
    } else clusterConfigWithWorkerConfigs
  }

  private def getPrimaryWorkerConfig(createClusterConfig: CreateClusterConfig): InstanceGroupConfig = {
    val workerDiskConfig = new DiskConfig()
      .setBootDiskSizeGb(createClusterConfig.machineConfig.workerDiskSize.get)
      .setNumLocalSsds(createClusterConfig.machineConfig.numberOfWorkerLocalSSDs.get)

    val workerConfig = new InstanceGroupConfig()
      .setNumInstances(createClusterConfig.machineConfig.numberOfWorkers.get)
      .setMachineTypeUri(createClusterConfig.machineConfig.workerMachineType.get)
      .setDiskConfig(workerDiskConfig)

    // If a custom dataproc image is specified, set it in the InstanceGroupConfig.
    // This overrides the imageVersion set in SoftwareConfig.
    createClusterConfig.dataprocCustomImage.foreach { customImage =>
      workerConfig.setImageUri(customImage)
    }

    workerConfig
  }

  /**
   * Gets a dataproc Cluster from the API.
   */
  private def getCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[DataprocCluster]] = {
    val request = dataproc.projects().regions().clusters().get(googleProject.value, defaultRegion, clusterName.value)
    retryWithRecover(retryPredicates: _*) { () =>
      Option(executeGoogleRequest(request))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  /**
   * Gets an Operation from the API.
   */
  private def getOperation(operationName: OperationName): Future[Option[DataprocOperation]] = {
    val request = dataproc.projects().regions().operations().get(operationName.value)
    retryWithRecover(retryPredicates: _*) { () =>
      Option(executeGoogleRequest(request))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  private def getOperationUUID(dop: DataprocOperation): UUID =
    UUID.fromString(dop.getMetadata.get("clusterUuid").toString)

  /**
   * Gets the master instance name from a dataproc cluster, with error handling.
   * @param cluster the Google dataproc cluster
   * @return error or master instance name
   */
  private def getMasterInstanceName(cluster: DataprocCluster): Option[InstanceName] =
    for {
      config <- Option(cluster.getConfig)
      masterConfig <- Option(config.getMasterConfig)
      instanceNames <- Option(masterConfig.getInstanceNames)
      masterInstance <- instanceNames.asScala.headOption
    } yield InstanceName(masterInstance)

  private def getAllInstanceNames(cluster: DataprocCluster): Option[Map[DataprocRole, Set[InstanceName]]] = {
    def getFromGroup(key: DataprocRole)(group: InstanceGroupConfig): Option[Map[DataprocRole, Set[InstanceName]]] =
      Option(group.getInstanceNames).map(_.asScala.toSet.map(InstanceName)).map(ins => Map(key -> ins))

    Option(cluster.getConfig).flatMap { config =>
      val masters = Option(config.getMasterConfig).flatMap(getFromGroup(Master))
      val workers = Option(config.getWorkerConfig).flatMap(getFromGroup(Worker))
      val secondaryWorkers = Option(config.getSecondaryWorkerConfig).flatMap(getFromGroup(SecondaryWorker))

      masters |+| workers |+| secondaryWorkers
    }
  }

  /**
   * Gets the zone (not to be confused with region) of a dataproc cluster, with error handling.
   * @param cluster the Google dataproc cluster
   * @return error or the master instance zone
   */
  private def getZone(cluster: DataprocCluster): Option[ZoneUri] = {
    def parseZone(zoneUri: String): ZoneUri =
      zoneUri.lastIndexOf('/') match {
        case -1 => ZoneUri(zoneUri)
        case n  => ZoneUri(zoneUri.substring(n + 1))
      }

    for {
      config <- Option(cluster.getConfig)
      gceConfig <- Option(config.getGceClusterConfig)
      zoneUri <- Option(gceConfig.getZoneUri)
    } yield parseZone(zoneUri)
  }

  //Note that this conversion will shave off anything smaller than a second in magnitude
  private def finiteDurationToGoogleDuration(duration: FiniteDuration): String =
    s"${duration.toSeconds}s"

  implicit private class GoogleExceptionSupport[A](future: Future[A]) {
    def handleGoogleException(project: GoogleProject, context: Option[String] = None): Future[A] =
      future.recover {
        case e: GoogleJsonResponseException =>
          val msg =
            s"Call to Google API failed for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Status: ${e.getStatusCode}. Message: ${e.getDetails.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
        case e: IllegalArgumentException =>
          val msg =
            s"Illegal argument passed to Google request for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Message: ${e.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
      }

    def handleGoogleException(project: GoogleProject, clusterName: ClusterName): Future[A] =
      handleGoogleException(project, Some(clusterName.value))
  }
}
