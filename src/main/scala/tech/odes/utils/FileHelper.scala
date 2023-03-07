package tech.odes.utils

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

object FileHelper extends Logging {
  def deleteFileQuietly(file: File): Path = {
    Files.walkFileTree(file.toPath, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        try {
          Files.delete(file)
        } catch {
          case t: Throwable => log.warn("Failed to delete", t)
        }
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        try {
          Files.delete(dir)
        } catch {
          case t: Throwable => log.warn("Failed to delete", t)
        }
        FileVisitResult.CONTINUE
      }
    })
  }
}
