package com.tune

import java.io.{File, FileInputStream, DataInputStream, BufferedInputStream, PrintWriter}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}


object ParquetDumper {
  def main(args: Array[String]) {
    import StdinUnpacker._
    org.apache.log4j.LogManager.getRootLogger.setLevel(org.apache.log4j.Level.ERROR)
    val out = new PrintWriter(System.out, true)
    val system = ActorSystem("ParquetDumper")
    val printer = system.actorOf(Props(new Printer(out)), "PrinterActor")
    val unpacker = system.actorOf(Props(new StdinUnpacker(printer)), "UnpackerActor")

    unpacker ! Unpack
  }
}

object StdinUnpacker {
  /* Message to tell the unpacker to start unpacking */
  case class Unpack()
  /* Message signaling the unpacker is finished processing */
  case class UnpackFinished()
}

/**
 * Actor for slicing parquet files out of a stream of
 * many parquet files
 */
class StdinUnpacker(printerActor: ActorRef) extends Actor {
  import ParquetReaderActor._
  import StdinUnpacker._

  val finishedReaders = new java.util.concurrent.atomic.AtomicInteger(0)
  var unpackFinished = false
  def receive = {
    case UnpackFinished => {
      unpackFinished = true
    }

    case ParquetReaderFinished => {
      val finished = finishedReaders.incrementAndGet
      val numReaders = context.children.size
      if (unpackFinished && numReaders == finished) {
        context.children.foreach(child => context.stop(child))
        context.stop(printerActor)
        context.stop(self)
        context.system.terminate
      }
    }

    case Unpack => {
      val stdin = new DataInputStream(new BufferedInputStream(new FileInputStream(new File("/dev/stdin"))))

      var currentFile : java.io.File = java.io.File.createTempFile("parquet-dumper", ".parquet")
      var currentStream : java.io.DataOutputStream = new java.io.DataOutputStream( new java.io.BufferedOutputStream( new java.io.FileOutputStream(currentFile)))
      var atStart = true
      var numParOnes = 1
      var numBytes = 0
      var sawParquetMrVersion = false
      val PAR1_BEG = "PAR1".toList.map(_.toByte).toVector
      val PAR1_END = Vector[Byte](0.toByte, 0.toByte) ++ PAR1_BEG
      var PARQUET_MR_VERSION = "parquet-mr version".toList.map(_.toByte).toVector
      val q = scala.collection.mutable.Queue[Byte]()

      Stream.continually[java.lang.Byte]({
            try {
              stdin.readByte
            } catch {
              case e: java.io.EOFException => null
              case e: Throwable => throw e
            }
      })
      .takeWhile(x => x != null)
      .foreach(b => {
        if (q.size >= 18) {
          q.dequeue
        }
        q += b
        numBytes+=1
        currentStream.writeByte(b.intValue)
        if (!sawParquetMrVersion) {
          sawParquetMrVersion = q.toVector == PARQUET_MR_VERSION
        }
        if ((q.toVector.endsWith(PAR1_BEG) && numParOnes % 2 == 1) || (q.toVector.endsWith(PAR1_END) && sawParquetMrVersion)) {
          if (numParOnes % 2 == 0) {
            currentStream.close
            context.actorOf(Props(new ParquetReaderActor(printerActor)), f"ParquetReaderActor_${currentFile.getPath.replaceAll("/","_")}") ! ParquetFile(currentFile.getPath)
            currentFile = java.io.File.createTempFile("parquet-dumper", ".parquet")
            currentStream = new java.io.DataOutputStream( new java.io.BufferedOutputStream( new java.io.FileOutputStream(currentFile)))
          } else {
            sawParquetMrVersion = false
          }
          numParOnes += 1
        }
      })
      stdin.close
      self ! UnpackFinished
    }
  }
}

object ParquetReaderActor {
  /**
   * message signaling a path to be read
   */
  case class ParquetFile(path: String)
  /* Message signaling a single parquet reader has finished */
  case class ParquetReaderFinished()
}

/*
 * Actor for handling the read of parquet files
 */
class ParquetReaderActor(printerActor: ActorRef) extends Actor {
  import ParquetReaderActor._
  import Printer._

  var linesSent = 0
  var linesProcessed = 0
  def receive = {
    /*
     * receives an acknowledgement from the
     * print actor that the line has been printed
     */
    case LineProcessed => {
      linesProcessed += 1
      if (linesProcessed == linesSent) {
        context.parent.tell(ParquetReaderFinished, self)
      }
    }

    /*
     * receives messages from the StdinUnpacker
     * with paths to files to read from parquet
     */
    case ParquetFile(path) => {
      val reader = ParquetReader.builder(new SimpleReadSupport(), new Path(path)).build()
      Stream.continually({
        reader.read()
      })
      .takeWhile(_!=null)
      .foreach(v => {
        printerActor ! Printer.ParquetRecord(v)
        linesSent += 1
      })
      new java.io.File(path).delete
    }
  }
}

object Printer {
  /**
   * Message received by Printer
   */
  case class ParquetRecord(record: SimpleRecord)
  /**
   * Message sent by Printer to notify
   * it has printed a line
   */
  case class LineProcessed()
}

/*
 * Actor for printing parquet records to out
 */
class Printer(out: PrintWriter) extends Actor {
  import Printer._
  def receive = {
    /**
     * takes a simple record and outputs
     * it to out as JSON
     */
    case Printer.ParquetRecord(record: SimpleRecord) => {
      record.prettyPrintJson(out)
      out.println()
      sender ! LineProcessed
    }
  }
}


