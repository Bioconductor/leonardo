package org.broadinstitute.dsde.workbench.leonardo

import java.io.File

import org.broadinstitute.dsde.workbench.config.AuthToken
import org.openqa.selenium.WebDriver

import scala.util.Try

class NotebooksListPage(override val url: String)(override implicit val authToken: AuthToken, override implicit val webDriver: WebDriver)
  extends JupyterPage {

  override def open(implicit webDriver: WebDriver): NotebooksListPage = super.open.asInstanceOf[NotebooksListPage]

  val uploadNewButton: Query = cssSelector("[title='Click to browse for a file to upload.']")
  val finishUploadButton: Query = cssSelector("[class='btn btn-primary btn-xs upload_button']")
  val newButton: Query = cssSelector("[id='new-buttons']")
  val python2Link: Query = cssSelector("[title='Create a new notebook with Python 2']")

  def upload(file: File): Unit = {
    uploadNewButton.findElement.get.underlying.sendKeys(file.getAbsolutePath)
    click on (await enabled finishUploadButton)
  }

  def withOpenNotebook[T](file: File)(testCode: NotebookPage => T): T = {
    await enabled text(file.getName)
    val notebookPage = new NotebookPage(url + "/notebooks/" + file.getName).open
    val result = Try { testCode(notebookPage) }
    notebookPage.shutdownKernel()
    result.get
  }

  def withNewNotebook[T](testCode: NotebookPage => T): T = {
    switchToNewTab {
      click on (await enabled newButton)
      click on (await enabled python2Link)
    }
    // Not calling NotebookPage.open() as it should already be opened
    val notebookPage = new NotebookPage(currentUrl)
    val result = Try { testCode(notebookPage) }
    notebookPage.shutdownKernel()
    result.get
  }
}