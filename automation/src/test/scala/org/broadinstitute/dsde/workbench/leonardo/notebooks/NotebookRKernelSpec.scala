package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.DoNotDiscover

import scala.concurrent.duration._

/**
 * This spec verifies notebook functionality specifically around the R kernel.
 */
@DoNotDiscover
class NotebookRKernelSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override val jupyterDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.rImageUrl)
  "NotebookRKernelSpec" - {

    // See https://github.com/DataBiosphere/leonardo/issues/398
    "should use UTF-8 encoding" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // Check the locale is set to en_US.UTF-8
          notebookPage.executeCell("""Sys.getenv("LC_ALL")""") shouldBe Some("'en_US.UTF-8'")

          // Make sure unicode characters display correctly
          notebookPage.executeCell("""install.packages("skimr")""", timeout = 5.minutes)
          notebookPage.executeCell("library(skimr)")

          val output = notebookPage.executeCell("""data(iris)
                                                  |skim(iris)""".stripMargin)

          output shouldBe 'defined
          output.get should not include ("<U+")
          output.get should include("▂▇▅▇▆▅▂▂")
        }
      }
    }

    // TODO: temporarily ignored. This was failing because we install SparkR based on Spark 2.2.3, but
    // Dataproc is giving us Spark 2.2.1. However this chart indicates that we should be getting Spark 2.2.3:
    // https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.2.
    // Opening a Google ticket and temporarily ignoring this test.
    "should create a notebook with a working R kernel and import installed packages" ignore { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell("library(SparkR)").get should include("SparkR")
          notebookPage.executeCell("sparkR.session()")
          notebookPage.executeCell("df <- as.DataFrame(faithful)")
          notebookPage.executeCell("head(df)").get should include("3.600 79")

          val sparkJob =
            """samples <- 200
              |inside <- function(index) {
              |  set.seed(index)
              |  rand <- runif(2, 0.0, 1.0)
              |  sum(rand^2) < 1
              |}
              |res <- spark.lapply(c(1:samples), inside)
              |pi <- length(which(unlist(res)))*4.0/samples
              |cat("Pi is roughly", pi, "\n")""".stripMargin

          notebookPage.executeCell(sparkJob).get should include("Pi is roughly ")
        }
      }
    }

    "should be able to install new R packages" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // httr is a simple http library for R
          // http://httr.r-lib.org//index.html

          // it may take a little while to install
          val installTimeout = 2.minutes

          val output = notebookPage.executeCell("""install.packages("httr")""", installTimeout)
          output shouldBe 'defined
          output.get should include("Installing package into")
          output.get should include("/home/jupyter-user/.rpackages")

          val httpGetTest =
            """library(httr)
              |r <- GET("http://www.example.com")
              |status_code(r)
            """.stripMargin

          notebookPage.executeCell(httpGetTest) shouldBe Some("200")
        }
      }
    }

    // See https://github.com/DataBiosphere/leonardo/issues/398
    "should be able to install mlr" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          // mlr: machine learning in R
          // https://github.com/mlr-org/mlr

          // it may take a little while to install
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""devtools::install_github("mlr-org/mlr")""", installTimeout)
          installOutput shouldBe 'defined
          installOutput.get should include("Downloading GitHub repo mlr-org/mlr@master")
          installOutput.get should not include ("Installation failed")

          // Make sure it was installed correctly; if not, this will return an error
          notebookPage.executeCell("library(mlr)").get should include("Loading required package: ParamHelpers")
          notebookPage.executeCell(""""mlr" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have tidyverse automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""tidyverse" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    "should have Ronaldo automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""Ronaldo" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }

    // See https://github.com/DataBiosphere/leonardo/issues/710
    "should be able to install packages that depend on gfortran" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          val installTimeout = 5.minutes

          val installOutput = notebookPage.executeCell("""install.packages('qwraps2')""", installTimeout)
          installOutput shouldBe 'defined
          installOutput.get should include("RcppArmadillo")
          installOutput.get should include("Installing package into")
          installOutput.get should include("/home/jupyter-user/.rpackages")
          installOutput.get should not include ("cannot find -lgfortran")
        }
      }
    }

    s"should have the workspace-related environment variables set" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebookInSubfolder(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage
            .executeCell("Sys.getenv('GOOGLE_PROJECT')")
            .get shouldBe s"'${clusterFixture.cluster.googleProject.value}'"
          notebookPage
            .executeCell("Sys.getenv('WORKSPACE_NAMESPACE')")
            .get shouldBe s"'${clusterFixture.cluster.googleProject.value}'"
          notebookPage.executeCell("Sys.getenv('WORKSPACE_NAME')").get shouldBe "'Untitled Folder'"
          notebookPage.executeCell("Sys.getenv('OWNER_EMAIL')").get shouldBe s"'${ronEmail}'"
          // workspace bucket is not wired up in tests
          notebookPage.executeCell("Sys.getenv('WORKSPACE_BUCKET')").get shouldBe "''"
        }
      }
    }
  }

}
