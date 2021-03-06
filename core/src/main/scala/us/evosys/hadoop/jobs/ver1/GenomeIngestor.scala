package us.evosys.hadoop.jobs.ver1

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
import collection.Iterator
import us.evosys.hadoop.jobs.{HImplicits, NonSplitableInputFormat}


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

    val job: Job = new Job(conf, "Genome Ingestor Ver1")

    job.setInputFormatClass(classOf[NonSplitableInputFormat])

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[GenomeIngestionMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NTermWritable])

    job.setCombinerClass(classOf[GenomeIngestionCombiner])

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

//class MultiFileOutput extends MultipleTextOutputFormat[]



object GenomeIngestionMapper {
  val WINDOW_SIZE = "window.size"
}

protected[ver1] class GenomeIngestionMapper extends Mapper[LongWritable, Text, Text, NTermWritable] with HImplicits with GImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var started: Boolean = false
  var stub: List[Char] = List()
  var winSize: Int = _
  var window: Window = _

  protected override def map(lnNumber: LongWritable, line: Text, context: Mapper[LongWritable, Text, Text, NTermWritable]#Context): Unit = {
    //System.err.println("file is " + context.getConfiguration.get("map.input.file"))
    val flName = context.getInputSplit match {
      case fs: FileSplit => fs.getPath.getName
      case _ => ""
    }

    //System.err.println(String.format("%s: processing line %s", flName, line))

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

//    val flOffset = context.getInputSplit.asInstanceOf[FileSplit].getStart
//    logger.trace("file Offset {}", flOffset)

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
          //System.err.println("char: " + c + ", term: " + term)
          context.write(term.value, term)
        case None =>
      }
      )
  }
}

protected[ver1] class GenomeIngestionCombiner extends Reducer[Text, NTermWritable, Text, NTermWritable] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  protected override def reduce(key: Text, value: java.lang.Iterable[NTermWritable], context: Reducer[Text, NTermWritable, Text, NTermWritable]#Context): Unit = {
    val nTerm = new NTermWritable(key)
    /* Don't know why the iterators are mixed */
    val filtered = value.filter(t => t.name.equalsIgnoreCase(key))
    System.out.println(key + " size of valueItr: " + value.size + ", filtered size: " + filtered.size)

    filtered.foldLeft(nTerm)((res, curr) => res.add(curr))
    System.err.println(String.format("combined %s %s", key, nTerm))

    context.write(key, nTerm)
  }
}

protected[jobs] class GenomeIngestionReducer extends Reducer[Text, NTermWritable, Text, NLink] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  protected override def reduce(key: Text, value: java.lang.Iterable[NTermWritable], context: Reducer[Text, NTermWritable, Text, NLink]#Context): Unit = {
    System.err.println("reducing {}", key)

    val nlink = NLink(key)

    value.foldLeft(nlink)((res, curr) => res.add(curr))

    context.write(key, nlink)
  }
}

protected[ver1] object NLink {
  def fromString(s: String): NLink = {
    val link = new NLink
    fromString(s, link)
    link
  }

  def fromString(s: String, link: NLink) {
    s split (":") match {
      case a: Array[String] if (a.size == 3) => {
        link.name = a(0)
        link.freq = a(1).toInt

        var locations: Map[String, List[Long]] = Map()

        a(2).tail match {
          case s: String => s.split(";") foreach {
            t => t.split("->") match {
              case e: Array[String] if (e.size == 2) =>
                val intArr: Array[Long] = e(1).split(",") match {
                  case strArr: Array[String] => for (s <- strArr) yield s.toLong
                  case _ => throw new IllegalArgumentException("unexpected " + e(1))
                }

                locations += (e(0) -> intArr.toList)
            }
          }
        }
        //System.out.println("locations: " + locations)
        link.locations = locations
      }
      case _ => throw new IllegalArgumentException("parse error: " + s)
    }
    link
  }
}

protected[ver1]  case class NLink(var name: String = "") extends Writable {
  def this() = this ("")

  private var freq: Long = 0

  def getFreq = freq

  var locations: Map[String, List[Long]] = Map()

  def getLocations: Map[String, List[Long]] = locations

  /* go through the termList and add all */
  def add(term: NTermWritable): NLink = {
    term.termList.foreach(term => {
      //System.err.println("adding " + term)
      term.value.equalsIgnoreCase(name) match {
        case true =>
          val l = term.offset :: locations.getOrElse(term.flName, List[Long]())

          locations = locations + (term.flName -> l)
          freq += term.freq
        case _ => //System.err.println(String.format("Link %s Skipping %s Expected %s Found %s", this, term, name, term.value))
      }
    })
    this
  }

  def write(out: DataOutput) {
    val str = toString
    //System.err.println("NLink writeOut: " + str)
    WritableUtils.writeString(out, str)
  }

  def readFields(in: DataInput) {
    val line = WritableUtils.readString(in)
    //System.err.println("NLink readIn: " + line)
    NLink.fromString(line, this)
  }

  override def toString = {
    var s: String = name.toUpperCase + ":" + freq + ":["

    locations.foreach(t => {
      s = s + t._1 + "->" + t._2.sorted.mkString(",") + ";"
    })

    s.take(s.size - 1)
  }
}

protected[ver1]  case class Window(size: Int) {
  var offset: Long = 0 - size
  var w: List[Char] = List()
  val re: Regex = "(?i)A|T|G|C".r
  val charSeq: Array[Char] = new Array(1)

  def slide(c: Char): Option[NTerm] = {
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

protected[ver1] class NTermWritable(var name: String) extends Writable {
  def this() = this ("")

  var termList: Set[NTerm] = Set()

  def add(term: NTermWritable): NTermWritable = {
    this.name.equalsIgnoreCase(term.name) match {
      case true => term.termList.foreach(t => t.value.equalsIgnoreCase(this.name) match {
        case true => termList += t
        case false => //System.err.println("Skipping: Expected term " + this.name + " found " + t.value)
      })
      case false => throw new IllegalArgumentException("Expected term " + this.name + " found " + term.name)
    }
    this
  }

  override def write(out: DataOutput) {
    System.err.println("Going to write " + name + " terms " + termList.size)
    WritableUtils.writeString(out, name)
    WritableUtils.writeVInt(out, termList.size)
    termList.foreach(t => t.write(out))
  }

  override def readFields(in: DataInput) {
    val tmp = new NTermWritable()
    tmp.name = WritableUtils.readString(in)
    for (i <- 0 until WritableUtils.readVInt(in)) tmp.termList += NTerm.fromString(WritableUtils.readString(in))
    System.err.println("just read " + name + " terms " + tmp.termList.size)

    this.name = tmp.name
    this.termList = tmp.termList
  }

  override def toString: String = termList.mkString
}

protected[ver1] object NTerm {
  def fromString(str: String): NTerm = populate(new NTerm(), str)

  def populate(nterm: NTerm, str: String): NTerm = {
    str split (":") match {
      case a: Array[String] if a.length == 4 =>
        nterm.value = a(0).trim; nterm.freq = a(1).trim.toInt; nterm.flName = a(2).trim; nterm.offset = a(3).trim.toInt
      case _ => throw new IllegalStateException("parse error:" + str)
    }
    nterm
  }
}

protected[ver1] class NTerm(var value: String, var freq: Long, var flName: String, var offset: Long) extends Ordered[NTerm] {
  def this() = this (null, 1, "", 0)

  def this(value: String, flName: String, offset: Long) = this (value, 1, flName, offset)

  def this(value: String, offset: Long) = this (value, 1, "", offset)

  override def equals(other: Any) = other match {
    case that: NTerm => equality.equals(that.equality)
    case _ => false
  }

  def write(out: DataOutput) {
    val str = value + ":" + freq + ":" + flName + ":" + offset
    //System.err.println("NTerm writeOut: " + str)
    WritableUtils.writeString(out, str)
  }

  def readFields(in: DataInput) {
    val line = WritableUtils.readString(in)
    NTerm.populate(this, line)
  }

  override def hashCode() = equality.hashCode

  def compare(that: NTerm): Int = {
    if (this.value.equalsIgnoreCase(that.value)) {
      if (this.flName.equalsIgnoreCase(that.flName)) {
        this.offset.compareTo(that.offset)
      }
      else {
        this.flName.compare(that.flName)
      }
    }
    else
      this.value.compare(that.value)
  }

  override def toString = "name: " + value + ", freq: " + freq + ", flName: " + flName + ", offset: " + offset

  private def equality = value.toUpperCase + flName + offset
}
