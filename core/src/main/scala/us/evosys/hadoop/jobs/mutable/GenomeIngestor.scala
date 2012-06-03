package us.evosys.hadoop.jobs.mutable

import org.apache.hadoop.conf.{Configured}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.mapreduce._
import java.lang.IllegalStateException
import java.io.{DataOutput, DataInput}
import org.apache.hadoop.io.{WritableUtils, LongWritable, Writable, Text}
import util.matching.Regex
import us.evosys.hadoop.jobs.{NonSplitableInputFormat, HImplicits}
import collection.mutable.LinkedList
import java.util.ArrayList


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


    val job: Job = new Job(conf, "Genome Ingestor Mutable")

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

protected[mutable] object GenomeIngestionMapper {
  val WINDOW_SIZE = "window.size"
}

protected[mutable] class GenomeIngestionMapper extends Mapper[LongWritable, Text, Text, NLink] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var started: Boolean = false
  var stub:String = ""
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

    val sb = new StringBuilder().append(stub).append(line.toUpperCase)

    //set the stub to used for next iteration
    stub = sb.substring (sb.length -(winSize-1))

    //System.err.println(String.format("effLine %s, stub: %s", effLine, stub))

    sb foreach (c =>
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

protected[mutable] class GenomeIngestionReducer extends Reducer[Text, NLink, Text, NLink] with HImplicits {
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

protected[mutable] case class NLink(var name: String = "", var chrName: String ="") extends Writable {
  def this() = this ("","")
  def this(name:String) = this(name, "")

  var locations = new ArrayList[Int]

  def getFreq = locations.size

  def add(term: NTerm): NLink = {
    term.value.equalsIgnoreCase(name) && term.flName.equalsIgnoreCase(chrName)  match {
      case true =>
        locations.add(term.offset)
      case _ =>
        throw new IllegalArgumentException("Expected term " + name + ":" + chrName + " found " + term.value + ":" + term.flName)
    }
    this
  }

  def add(link: NLink) = {
    link.name.equalsIgnoreCase(name) && link.chrName.equalsIgnoreCase(chrName)  match {
      case true =>
        link.locations.foreach(locations.add(_))
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
    for (i <- 0 until size) tmp.locations.add(WritableUtils.readVInt(in))

    this.name = tmp.name
    this.chrName = tmp.chrName
    this.locations = tmp.locations
  }

  override def toString = {
    locations.size+":"+locations.sorted.mkString(",")
  }
}

protected[mutable] case class Window(size: Int) {
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

protected[mutable] class NTerm(var value: String, var flName: String, var offset: Int) extends Ordered[NTerm] {
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




