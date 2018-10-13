package chrism.sdsc.hadoop

import java.io.IOException
import java.nio.{file => jnf}
import java.{io => jio}

import chrism.sdsc.TestSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FileSystem, FileUtil}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, SuiteMixin}

trait TestHadoopFileSystemMixin extends SuiteMixin with BeforeAndAfterAll {
  this: TestSuite =>

  import TestHadoopFileSystemMixin._

  protected final val hdfsPathSeparator: String = fs.Path.SEPARATOR
  protected final lazy val hdfsRoot: String = hdfsPathSeparator

  private[this] final lazy val _baseDir: jio.File =
    jnf.Files.createTempDirectory("test_hdfs").toFile.getAbsoluteFile

  private[this] final lazy val hdfsSchemeAuthority: String =
    "hdfs://localhost:" + getOrInitializeCluster().getNameNodePort

  private[this] var _hadoopConf: Configuration = _
  private[this] var _cluster: MiniDFSCluster = _
  private[this] var _hdfs: FileSystem = _
  private[this] var _closed: Boolean = false

  protected final def hdfsPutResource(resourcePath: String, outputPath: fs.Path): Unit = {
    import chrism.sdsc.util.ResourceHandle.using

    using(getClass.getResourceAsStream(resourcePath))(iStream => {
      val buffer = new Array[Byte](iStream.available())
      new Array[Byte](iStream.available())
      iStream.read(buffer)

      val tempFile = jio.File.createTempFile(FilePrefix, null)
      tempFile.deleteOnExit()

      using(new jio.FileOutputStream(tempFile))(oStream => oStream.write(buffer))

      val inputPath = new fs.Path(tempFile.getAbsoluteFile.getAbsolutePath)
      hdfs().copyFromLocalFile(inputPath, outputPath)
    })
  }

  protected final def hdfs(/* side-effects */): FileSystem = getOrInitializeFileSystem()

  protected final def newHdfsPath(p: String, subPaths: String*): fs.Path =
    new fs.Path((hdfsSchemeAuthority +: p +: subPaths).mkString(hdfsPathSeparator))

  private[this] def getOrInitializeHadoopConf(/* IO */): Configuration = {
    if (_hadoopConf == null) {
      _hadoopConf = new Configuration()
      _hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, _baseDir.getAbsolutePath)
    }
    _hadoopConf
  }

  private[this] def close(/* side-effects */): Unit = {
    if (_closed) {
      throw new IOException("FileSystem cannot be re-closed when it is already closed!")
    }

    _hdfs = null
    _cluster.shutdown()
    _cluster = null
    FileUtil.fullyDelete(_baseDir)
    _closed = true
  }

  private[this] def checkClosed(/* side-effects */): Unit = {
    if (_closed) {
      throw new IOException("FileSystem has already been closed!")
    }
  }

  private[this] def getOrInitializeCluster(/* side-effects */): MiniDFSCluster = {
    checkClosed()
    if (_cluster == null) {
      _cluster = new MiniDFSCluster.Builder(getOrInitializeHadoopConf()).build()
    }
    _cluster
  }

  private[this] def getOrInitializeFileSystem(/* side-effects */): FileSystem = {
    val cluster = getOrInitializeCluster()
    if (_hdfs == null) {
      _hdfs = cluster.getFileSystem(/* IO */)
    }
    _hdfs
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    close()
    _hadoopConf = null
  }
}

private[this] object TestHadoopFileSystemMixin {

  private val FilePrefix: String = "hdfs_test_"
}