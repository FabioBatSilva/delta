/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.nio.file.Paths

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.util.Utils

/**
 * Information about a manifest.
 *
 * @param path Path to the manifest file
 */
case class DeltaManifest(
   path: Path
)

/**
 * General interface writing manifest files for a delta table.
 */
trait DeltaManifestWriter {
  def write(fs: FileSystem, snapshot: Snapshot): Seq[DeltaManifest]

  protected def groupByPartitions(snapshot: Snapshot): Map[Path, Iterable[AddFile]] = {
    snapshot.allFiles.collect().seq
      .groupBy(r => new Path(Paths.get(r.path).getParent.toString))
  }

  protected def writeInternal(fs: FileSystem, path: Path, content: String): Unit = {
    var streamClosed = false
    val stream = fs.create(path, false)

    try {
      if (!fs.exists(path.getParent)) {
        fs.mkdirs(path.getParent)
      }

      stream.writeBytes(content)
      stream.close()

      streamClosed = true

    } finally {
      if (!streamClosed) {
        stream.close()
      }
    }
  }
}

/**
 * Writes SymlinkTextInputFormat manifests.
 */
class SymlinkTextInputFormatWriter extends DeltaManifestWriter {
  private def createManifestPath(dataPath: Path, fragment: Path): Path = {
    val manifestPath = new Path(dataPath, "_symlink_format_manifest")
    val fragmentPath = new Path(manifestPath, fragment)

    new Path(fragmentPath, "manifest.txt")
  }

  private def createManifestContent(dataPath: Path, files: Iterable[AddFile]): String = {
    files
      .map(f => new Path(dataPath, f.path))
      .mkString("\n")
  }

  override def write(fs: FileSystem, snapshot: Snapshot): Seq[DeltaManifest] = {
    val groups = groupByPartitions(snapshot)
    val dataPath = snapshot.deltaLog.dataPath

    groups
      .map(e => {
        val path = createManifestPath(dataPath, e._1)
        val content = createManifestContent(dataPath, e._2)

        writeInternal(fs, path, content)

        path
      })
      .map(DeltaManifest)
      .toSeq
  }

}

trait ManifestWriterProvider {
  val writerClassConfKey: String = "spark.delta.manifest.writer"
  val defaultWriterClassName: String = classOf[SymlinkTextInputFormatWriter].getName

  def createManifestWriter(spark: SparkSession): DeltaManifestWriter = {
    val context = spark.sparkContext
    val sparkConf = context.getConf

    createManifestWriter(sparkConf)
  }

  def createManifestWriter(sparkConf: SparkConf): DeltaManifestWriter = {
    val writerClassName = sparkConf.get(writerClassConfKey, defaultWriterClassName)
    val writerClass = Utils.classForName(writerClassName).asInstanceOf[Class[DeltaManifestWriter]]

    createManifestWriter(writerClass)
  }

  def createManifestWriter(writerClass: Class[DeltaManifestWriter]): DeltaManifestWriter = {
    writerClass.getConstructor()
      .newInstance()
      .asInstanceOf[DeltaManifestWriter]
  }
}
