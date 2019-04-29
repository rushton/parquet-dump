package parquetdump

import scala.collection.JavaConverters._

import java.io.{File, FileInputStream, DataInputStream, BufferedInputStream, PrintWriter, InputStream}
import java.util.concurrent.ArrayBlockingQueue
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

object ParquetDumper {
  def main(args: Array[String]) {
    val isCount = args.headOption.map(_ == "--counts").getOrElse(false)
    val wq = new ArrayBlockingQueue[SimpleRecord](1000)
    val in = new FileInputStream(new File("/dev/stdin"))
    try {
      val out = new PrintWriter(System.out, true)
      unpackInputStream(in)
        .foreach(filePath => {
          if (isCount) {
            val f = HadoopInputFile.fromPath(new Path(s"file://$filePath"), new org.apache.hadoop.conf.Configuration)
            val blocks = ParquetFileReader.readFooter(new org.apache.hadoop.conf.Configuration, new Path(s"file://$filePath"), ParquetMetadataConverter.NO_FILTER)
              .getBlocks
              .asScala
              .foreach(b => {
                  out.println(b.getRowCount)
              })
          } else {
            val reader = ParquetReader.builder(new SimpleReadSupport(), new Path(filePath)).build()
            Iterator.continually({
              reader.read()
            })
            .takeWhile(_ != null)
            .foreach(record => {
              record.prettyPrintJson(out)
              out.println()
            })
        }
        })
    } catch {
      case e: Throwable => throw e
    } finally {
      in.close
    }

  }
  def unpackInputStream(inp : InputStream) : Stream[String] = {
    val stdin = new DataInputStream(new BufferedInputStream(inp))
    Stream.continually({
      val q = scala.collection.mutable.Queue[Byte]()
      getParquetFile(stdin, q)
    })
    .takeWhile(_ != null)
  }

  def getParquetFile(inp: DataInputStream, q: scala.collection.mutable.Queue[Byte]) : String = {
    val PAR1_BEG = scala.collection.mutable.Queue("PAR1".toList.map(_.toByte): _*)
    val PAR1_END = scala.collection.mutable.Queue[Byte](0.toByte) ++ PAR1_BEG
    var PARQUET_MR_VERSION = "parquet-mr version".toList.map(_.toByte).toVector


    var currentFile : java.io.File = java.io.File.createTempFile("parquet-dumper", ".parquet")
    currentFile.deleteOnExit
    var currentStream : java.io.DataOutputStream = new java.io.DataOutputStream( new java.io.BufferedOutputStream( new java.io.FileOutputStream(currentFile)))
    var numParOnes = 1
    var sawParquetMrVersion = false

    var first = true
    var b: java.lang.Byte = null
    var currentPath : String = null


    while (b != null || first) {
      first = false
      try {
        b = inp.readByte
        if (q.size >= 18) {
          q.dequeue
        }
        q += b
        currentStream.writeByte(b.toInt)
        if (!sawParquetMrVersion && q.front == PARQUET_MR_VERSION.head) {
          sawParquetMrVersion = q.toVector == PARQUET_MR_VERSION
        }
        if ((numParOnes % 2 == 1 && q.endsWith(PAR1_BEG))) {
          sawParquetMrVersion = false
          numParOnes += 1
        } else if( sawParquetMrVersion && q.endsWith(PAR1_END)) {
          if (numParOnes % 2 == 0) {
            currentStream.close
            currentPath = currentFile.getPath
            b=null
          }
          numParOnes += 1
        }
      } catch {
        case e: java.io.EOFException => {
          b=null
        }
        case e: Throwable => throw e
      }
    }
    currentPath
  }
}
