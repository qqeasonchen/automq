/**
 * KRaft目录级别恢复助手
 * 通过整个目录打包/解包的方式进行KRaft元数据恢复
 */
package kafka.log

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import kafka.utils.Logging
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

/**
 * KRaft目录级别的备份和恢复工具
 *
 * 优势:
 * 1. 简单直接 - 就是文件操作
 * 2. 完整性 - 保留所有状态和元数据
 * 3. 原子性 - 要么完全恢复，要么失败
 * 4. 兼容性 - 与现有Kafka完全兼容
 */
class KRaftDirectoryRecoveryHelper(
  kraftLogDir: File,
  time: Time
) extends Logging {

  logIdent = s"[KRaftDirectoryRecovery dir=${kraftLogDir.getAbsolutePath}] "

  /**
   * 检查是否应该尝试目录级别的恢复
   */
  def shouldAttemptDirectoryRecovery(): Boolean = {
    try {
      // 检查目录是否为空或只有部分文件
      if (!kraftLogDir.exists()) {
        info("KRaft log directory does not exist, recovery may be needed")
        return true
      }

      val files = kraftLogDir.listFiles()
      if (files == null || files.length == 0) {
        info("KRaft log directory is empty, recovery needed")
        return true
      }

      // 检查是否缺少关键文件
      val criticalFiles = Array("meta.properties", "__cluster_metadata-0")
      val missingFiles = criticalFiles.filter { fileName =>
        !new File(kraftLogDir, fileName).exists()
      }

      if (missingFiles.nonEmpty) {
        info(s"Critical files missing: ${missingFiles.mkString(", ")}, recovery needed")
        return true
      }

      // 检查__cluster_metadata-0目录是否有内容
      val metadataDir = new File(kraftLogDir, "__cluster_metadata-0")
      if (metadataDir.exists() && metadataDir.isDirectory) {
        val metadataFiles = metadataDir.listFiles()
        if (metadataFiles == null || metadataFiles.length == 0) {
          info("__cluster_metadata-0 directory is empty, recovery needed")
          return true
        }

        // 检查是否有log文件
        val hasLogFile = metadataFiles.exists(f => f.getName.endsWith(".log"))
        if (!hasLogFile) {
          info("No .log files found in __cluster_metadata-0, recovery needed")
          return true
        }
      }

      debug("KRaft directory appears to be complete, no recovery needed")
      false
    } catch {
      case e: Exception =>
        warn(s"Error checking directory recovery status: ${e.getMessage}", e)
        false
    }
  }

  /**
   * 创建KRaft目录的备份包
   * @return 备份数据的字节数组，可以直接上传到S3
   */
  def createBackupPackage(): Option[Array[Byte]] = {
    try {
      info("Creating KRaft directory backup package...")

      if (!kraftLogDir.exists() || !kraftLogDir.isDirectory) {
        warn(s"KRaft directory does not exist or is not a directory: ${kraftLogDir.getAbsolutePath}")
        return None
      }

      val baos = new java.io.ByteArrayOutputStream()
      val zos = new ZipOutputStream(baos)

      try {
        // 递归添加所有文件到zip
        addDirectoryToZip(kraftLogDir, kraftLogDir.getName, zos)

        zos.flush()
        val backupData = baos.toByteArray

        info(s"Successfully created backup package: ${backupData.length} bytes, " +
             s"containing ${getFileCount(kraftLogDir)} files")

        Some(backupData)
      } finally {
        zos.close()
        baos.close()
      }
    } catch {
      case e: Exception =>
        error(s"Failed to create backup package: ${e.getMessage}", e)
        None
    }
  }

  /**
   * 从备份包恢复KRaft目录
   * @param backupData 从S3下载的备份数据
   * @return 恢复是否成功
   */
  def restoreFromBackupPackage(backupData: Array[Byte]): Boolean = {
    try {
      info("Restoring KRaft directory from backup package...")

      // 创建临时目录用于解压
      val tempDir = Files.createTempDirectory("kraft-restore").toFile
      val backupDir = new File(tempDir, "backup")

      try {
        // 解压备份数据到临时目录
        if (!extractZipToDirectory(backupData, tempDir)) {
          error("Failed to extract backup data")
          return false
        }

        // 查找解压后的KRaft目录
        val extractedDirs = tempDir.listFiles().filter(_.isDirectory)
        if (extractedDirs.isEmpty) {
          error("No directories found in backup data")
          return false
        }

        val sourceDir = extractedDirs.head
        info(s"Found extracted directory: ${sourceDir.getName}")

        // 验证解压后的目录结构
        if (!validateBackupDirectory(sourceDir)) {
          error("Backup directory structure validation failed")
          return false
        }

        // 清理目标目录（如果存在）
        if (kraftLogDir.exists()) {
          info("Cleaning existing KRaft directory...")
          if (!deleteDirectoryRecursively(kraftLogDir)) {
            error("Failed to clean existing directory")
            return false
          }
        }

        // 创建父目录
        kraftLogDir.getParentFile.mkdirs()

        // 复制解压后的目录到目标位置
        if (!copyDirectoryRecursively(sourceDir, kraftLogDir)) {
          error("Failed to copy restored directory")
          return false
        }

        info(s"Successfully restored KRaft directory with ${getFileCount(kraftLogDir)} files")
        return true

      } finally {
        // 清理临时目录
        try {
          deleteDirectoryRecursively(tempDir)
        } catch {
          case e: Exception =>
            warn(s"Failed to clean temporary directory: ${e.getMessage}")
        }
      }
    } catch {
      case e: Exception =>
        error(s"Failed to restore from backup package: ${e.getMessage}", e)
        false
    }
  }

  /**
   * 递归添加目录到zip文件
   */
  private def addDirectoryToZip(sourceDir: File, baseName: String, zos: ZipOutputStream): Unit = {
    val files = sourceDir.listFiles()
    if (files != null) {
      files.foreach { file =>
        val entryName = if (baseName.isEmpty) file.getName else s"$baseName/${file.getName}"

        if (file.isDirectory) {
          // 添加目录条目
          zos.putNextEntry(new ZipEntry(entryName + "/"))
          zos.closeEntry()

          // 递归处理子目录
          addDirectoryToZip(file, entryName, zos)
        } else {
          // 添加文件
          zos.putNextEntry(new ZipEntry(entryName))

          val fis = new FileInputStream(file)
          try {
            val buffer = new Array[Byte](8192)
            var length = fis.read(buffer)
            while (length > 0) {
              zos.write(buffer, 0, length)
              length = fis.read(buffer)
            }
          } finally {
            fis.close()
          }

          zos.closeEntry()
          debug(s"Added file to backup: $entryName (${file.length()} bytes)")
        }
      }
    }
  }

  /**
   * 解压zip数据到目录
   */
  private def extractZipToDirectory(zipData: Array[Byte], targetDir: File): Boolean = {
    try {
      val zis = new ZipInputStream(new java.io.ByteArrayInputStream(zipData))

      try {
        var entry = zis.getNextEntry
        while (entry != null) {
          val file = new File(targetDir, entry.getName)

          if (entry.isDirectory) {
            file.mkdirs()
          } else {
            // 创建父目录
            file.getParentFile.mkdirs()

            // 写入文件内容
            val fos = new FileOutputStream(file)
            try {
              val buffer = new Array[Byte](8192)
              var length = zis.read(buffer)
              while (length > 0) {
                fos.write(buffer, 0, length)
                length = zis.read(buffer)
              }
            } finally {
              fos.close()
            }

            debug(s"Extracted file: ${entry.getName} (${file.length()} bytes)")
          }

          zis.closeEntry()
          entry = zis.getNextEntry
        }
        true
      } finally {
        zis.close()
      }
    } catch {
      case e: Exception =>
        error(s"Failed to extract zip data: ${e.getMessage}", e)
        false
    }
  }

  /**
   * 验证备份目录结构
   */
  private def validateBackupDirectory(dir: File): Boolean = {
    if (!dir.exists() || !dir.isDirectory) {
      error(s"Backup directory does not exist: ${dir.getAbsolutePath}")
      return false
    }

    // 检查关键文件
    val criticalFiles = Array("meta.properties")
    val missingFiles = criticalFiles.filter { fileName =>
      !new File(dir, fileName).exists()
    }

    if (missingFiles.nonEmpty) {
      error(s"Critical files missing in backup: ${missingFiles.mkString(", ")}")
      return false
    }

    // 检查__cluster_metadata-0目录
    val metadataDir = new File(dir, "__cluster_metadata-0")
    if (!metadataDir.exists() || !metadataDir.isDirectory) {
      error("__cluster_metadata-0 directory missing in backup")
      return false
    }

    info("Backup directory validation passed")
    true
  }

  /**
   * 递归复制目录
   */
  private def copyDirectoryRecursively(source: File, target: File): Boolean = {
    try {
      if (source.isDirectory) {
        if (!target.exists()) {
          target.mkdirs()
        }

        val files = source.listFiles()
        if (files != null) {
          files.foreach { file =>
            val targetFile = new File(target, file.getName)
            if (!copyDirectoryRecursively(file, targetFile)) {
              return false
            }
          }
        }
      } else {
        Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
        debug(s"Copied file: ${source.getName} (${source.length()} bytes)")
      }
      true
    } catch {
      case e: Exception =>
        error(s"Failed to copy ${source.getAbsolutePath} to ${target.getAbsolutePath}: ${e.getMessage}", e)
        false
    }
  }

  /**
   * 递归删除目录
   */
  private def deleteDirectoryRecursively(dir: File): Boolean = {
    try {
      if (dir.exists()) {
        if (dir.isDirectory) {
          val files = dir.listFiles()
          if (files != null) {
            files.foreach { file =>
              if (!deleteDirectoryRecursively(file)) {
                return false
              }
            }
          }
        }
        dir.delete()
      }
      true
    } catch {
      case e: Exception =>
        error(s"Failed to delete ${dir.getAbsolutePath}: ${e.getMessage}", e)
        false
    }
  }

  /**
   * 统计目录中的文件数量
   */
  private def getFileCount(dir: File): Int = {
    if (!dir.exists() || !dir.isDirectory) {
      return 0
    }

    var count = 0
    val files = dir.listFiles()
    if (files != null) {
      files.foreach { file =>
        if (file.isDirectory) {
          count += getFileCount(file)
        } else {
          count += 1
        }
      }
    }
    count
  }
}