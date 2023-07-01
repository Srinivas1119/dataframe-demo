package com.knsoft.utils

import java.nio.file.{FileSystems, Files, Paths}
import scala.collection.JavaConverters._
object FileUtils {

  def renameReport(reportFolder:String):Unit={
    val dir = FileSystems.getDefault.getPath(reportFolder)
    val list = Files.list(dir).iterator().asScala.toList
    list.filterNot(a => a.getFileName.toString.endsWith(".csv")).foreach(a => {
      println(s"Deleting file at path : ${a}")
      val isDeleted = Files.deleteIfExists(a)
      if (isDeleted) println(s"deleted : ${a}")
      else println(s"not deleted : ${a}")
    })
    val path = list.filter(a => a.getFileName.toString.endsWith(".csv"))(0)
    val finalReportPath = Paths.get(s"${reportFolder}/employee.csv")
    Files.move(path, finalReportPath)
  }
}
