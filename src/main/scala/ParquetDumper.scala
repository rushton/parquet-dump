package com.tune

import java.io.{File, FileInputStream, DataInputStream, BufferedInputStream, PrintWriter}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import org.apache.parquet.hadoop.util.HadoopStreams
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

  private def maybeTerminate() {
    val numReaders = context.children.size
    if (unpackFinished && numReaders == finishedReaders.get) {
      context.children.foreach(child => context.stop(child))
      context.stop(printerActor)
      context.stop(self)
      context.system.terminate
    }
  }

  def receive = {
    case UnpackFinished => {
      unpackFinished = true
      maybeTerminate
    }

    case ParquetReaderFinished => maybeTerminate
    case Unpack => {
      val stdin = new DataInputStream(new BufferedInputStream(new FileInputStream(new File("/dev/stdin"))))

      var currentStream = scala.collection.mutable.ListBuffer[Byte]()
      var atStart = true
      var numParOnes = 1
      var sawParquetMrVersion = false
      val PAR1_BEG = scala.collection.mutable.Queue("PAR1".toList.map(_.toByte): _*)
      val PAR1_END = scala.collection.mutable.Queue[Byte](0.toByte, 0.toByte) ++ PAR1_BEG
      var PARQUET_MR_VERSION = "parquet-mr version".toList.map(_.toByte).toVector
      val q = scala.collection.mutable.Queue[Byte]()

      var first = true
      var b: java.lang.Byte = null
      while (b != null || first) {
            first = false
            try {
              b = stdin.readByte
              if (q.size >= 18) {
                q.dequeue
              }
              q += b
              currentStream += b
              if (!sawParquetMrVersion && q.front == PARQUET_MR_VERSION.head) {
                sawParquetMrVersion = q.toVector == PARQUET_MR_VERSION
              }
              if ((numParOnes % 2 == 1 && q.endsWith(PAR1_BEG))) {
                sawParquetMrVersion = false
                numParOnes += 1
              } else if( sawParquetMrVersion && q.endsWith(PAR1_END)) {
                if (numParOnes % 2 == 0) {
                  context.actorOf(
                    Props(new ParquetReaderActor(printerActor)),
                    f"ParquetReaderActor_${numParOnes/2}"
                  ) ! ParquetFile(
                    new InMemoryInputFile(
                      HadoopStreams.wrap(
                        new FSDataInputStream(
                          new SeekableByteArrayInputStream(currentStream.toArray)
                        )
                      ),
                      currentStream.size.toLong
                    )
                  )

                  currentStream = scala.collection.mutable.ListBuffer[Byte]()
                }
                numParOnes += 1
              }
            } catch {
              case e: java.io.EOFException => {
                b = null
              }
              case e: Throwable => throw e
            }
      }
      stdin.close
      self ! UnpackFinished
    }
  }
}

object ParquetReaderActor {
  /**
   * message signaling a path to be read
   */
  case class ParquetFile(input: InMemoryInputFile)
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
    case ParquetFile(input) => {
      val builder = new InMemoryBuilder[SimpleRecord](input)
      builder.useReadSupport(new SimpleReadSupport())

      val reader = builder.build
      Stream.continually({
        reader.read()
      })
      .takeWhile(_!=null)
      .foreach(v => {
        printerActor ! Printer.ParquetRecord(v)
        linesSent += 1
      })
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


