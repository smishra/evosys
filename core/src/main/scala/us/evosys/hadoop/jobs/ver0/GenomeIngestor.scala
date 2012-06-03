package us.evosys.hadoop.jobs.ver0

import org.apache.hadoop.conf.{Configured}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileSplit, FileInputFormat}
import org.apache.hadoop.mapreduce._
import java.lang.IllegalStateException
import java.io.{DataOutput, DataInput}
import org.apache.hadoop.io.{WritableUtils, LongWritable, Writable, Text}
import util.matching.Regex
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import us.evosys.hadoop.jobs.{NonSplitableInputFormat, HImplicits}


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/4/12
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */


object GenomeIngestor extends Configured with Tool with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(args: Array[String]): Int = {
    val conf = getConf
    conf.setQuietMode(false)
    conf.set(GenomeIngestionMapper.WINDOW_SIZE, 3 + "")


    val job: Job = new Job(conf, "Genome Ingestor Ver0")

    job.setInputFormatClass(classOf[NonSplitableInputFormat])

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[GenomeIngestionMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NLink])

    job.setCombinerClass(classOf[GenomeIngestionReducer])

    job.setReducerClass(classOf[GenomeIngestionReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NLink])
    //job.setNumReduceTasks(0)
    for (i <- 0 to args.length - 2) {
      logger.info("processing {}: {}", i, args(i))
      FileInputFormat.addInputPath(job, args.apply(i))
    }

    FileOutputFormat.setOutputPath(job, args.last)

    job.waitForCompletion(true) match {
      case true => 0
      case false => 1
    }
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, args))
  }
}

object GenomeIngestionMapper {
  val WINDOW_SIZE = "window.size"
}

protected[ver0] class GenomeIngestionMapper extends Mapper[LongWritable, Text, Text, NLink] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var started: Boolean = false
  var stub: List[Char] = List()
  var winSize: Int = _
  var window: Window = _

  protected override def map(lnNumber: LongWritable, line: Text, context: Mapper[LongWritable, Text, Text, NLink]#Context): Unit = {
    //System.err.println("file is " + context.getConfiguration.get("map.input.file"))
    val flName = context.getInputSplit match {
      case fs: FileSplit => fs.getPath.getName
      case _ => ""
    }

    //Initialize
    if (!started) {
      winSize = context.getConfiguration.get(GenomeIngestionMapper.WINDOW_SIZE) match {
        case s: String => Integer.parseInt(s)
        case _ => throw new IllegalStateException("Window size not set")
      }
      logger.info("window size {}", winSize)

      window = Window(winSize)

      started = true
    }

    //Ignore the non genome related line
    if (line.startsWith(">")) {
      logger.info("skipping {}", line)
      //System.err.println("skipping " + line)
      return
    }

    val effLine: String = stub.isEmpty match {
      case false => stub.mkString + line.toUpperCase
      case _ => line.toUpperCase
    }

    //set the stub to used for next iteration
    stub = line.toCharArray.takeRight(winSize - 1).toList

    //System.err.println(String.format("effLine %s, stub: %s", effLine, stub))

    effLine foreach (c =>
      window.slide(c) match {
        case some: Some[NTerm] =>
          val term = some.get
          term.flName = flName
          context.write(term.value+":"+flName, NLink(term.value, term.flName).add(term))
        case None =>
      }
      )
  }
}

protected[ver0] class GenomeIngestionReducer extends Reducer[Text, NLink, Text, NLink] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  var termLinkMap: Map[String, NLink] = Map()

  protected override def reduce(key: Text, value: java.lang.Iterable[NLink], context: Reducer[Text, NLink, Text, NLink]#Context): Unit = {
    logger.debug("reducing {}", key)
    val nmPair = key.split(":")
    val nlink = NLink(nmPair(0), nmPair(1))
    //value.foreach(nlink.add(_))
    value.foldLeft(nlink)((res, curr) => res.add(curr))

    context.write(key, nlink)
  }
}

case class NLink(var name: String = "", var chrName: String ="") extends Writable {
  def this() = this ("","")
  def this(name:String) = this(name, "")

  var locations: List[Int] = List()

  def getFreq = locations.size

  private var sorted: Boolean = false

  def getLocations: List[Int] = locations

  def add(term: NTerm): NLink = {
    term.value.equalsIgnoreCase(name) && term.flName.equalsIgnoreCase(chrName)  match {
      case true =>
        locations = term.offset :: locations
        sorted = false
      case _ =>
        throw new IllegalArgumentException("Expected term " + name + ":" + chrName + " found " + term.value + ":" + term.flName)
    }
    this
  }

  def add(link: NLink) = {
    link.name.equalsIgnoreCase(name) && link.chrName.equalsIgnoreCase(chrName)  match {
      case true =>
        locations = this.locations ::: link.locations
        sorted = false
      case _ =>
        throw new IllegalArgumentException("Expected term " + name + ":" + chrName + " found " + link.name + ":" + link.chrName)
    }
    this
  }

  def write(out: DataOutput) {
    WritableUtils.writeString(out,name)
    WritableUtils.writeString(out,chrName)
    //System.err.println("NLink writeOut: " + str)
    WritableUtils.writeVInt(out, locations.size)
    locations.foreach(WritableUtils.writeVInt(out, _))
  }

  def readFields(in: DataInput) {
    val tmp = new NLink
    tmp.name = WritableUtils.readString(in)
    tmp.chrName = WritableUtils.readString(in)
    val size = WritableUtils.readVInt(in)
    for (i <- 0 until size) tmp.locations = WritableUtils.readVInt(in) :: tmp.locations

    this.name = tmp.name
    this.chrName = tmp.chrName
    this.locations = tmp.locations
  }

  private def sort {
    if (!sorted) {
      locations = locations.sorted
      sorted = true
    }
  }

  override def toString = {
    sort
    locations.size+":"+locations.mkString(",")
  }
}

case class Window(size: Int) {
  var offset: Int = 0 - size
  var w: List[Char] = List()
  val re: Regex = "(?i)A|T|G|C".r
  val charSeq: Array[Char] = new Array(1)

  def slide(c:Char): Option[NTerm] = {
    charSeq(0) = c
    if (re.findFirstIn(charSeq).getOrElse("") != "") {
      offset += 1
      w ::= c
      if (w.size == size) {
        val value = w.reverse
        w = w.take(size - 1)

        return Some(new NTerm(value.mkString, offset))
      }
    }

    None
  }
}

class NTerm(var value: String, var flName: String, var offset: Int) extends Ordered[NTerm] {
  def this() = this (null, "", 0)

  def this(value: String, offset: Int) = this (value, "", offset)

  override def equals(other: Any) = other match {
    case that: NTerm => equality.equals(that.equality)
    case _ => false
  }

  override def hashCode() = equality.hashCode

  def compare(that: NTerm) = that.equality.compare(this.equality)

  override def toString = "name: " + value + ", flName: " + flName + ", offset: " + offset

  private def equality = value.toUpperCase + flName + offset
}


//class BufferedRecordReader(bufferSize: Int) extends RecordReader {
//  def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {}
//
//  def nextKeyValue() = false
//
//  def getCurrentKey = null
//
//  def getCurrentValue = null
//
//  def getProgress = 0.0
//
//  def close() {}
//}





