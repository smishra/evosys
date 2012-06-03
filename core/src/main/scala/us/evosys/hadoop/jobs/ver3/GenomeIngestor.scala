package us.evosys.hadoop.jobs.ver3

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
import us.evosys.hadoop.jobs.{NonSplitableInputFormat, HImplicits}
import us.evosys.hadoop.jobs.ver1.GenomeIngestionMapper.WINDOW_SIZE
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
    conf.set(WINDOW_SIZE, 3 + "")


    val job: Job = new Job(conf, "Genome Ingestor Mutable Node")

    job.setInputFormatClass(classOf[NonSplitableInputFormat])

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[GenomeIngestionMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[MutableNode])

    job.setCombinerClass(classOf[GenomeIngestionReducer])

    job.setReducerClass(classOf[GenomeIngestionReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[MutableNode])
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

protected[ver3] case class Window(size: Int) {
  var offset: Int = 0 - size
  var w: List[Char] = List()
  val re: Regex = "(?i)A|T|G|C".r
  val charSeq: Array[Char] = new Array(1)

  def slide(c: Char): Option[MutableNode] = {
    charSeq(0) = c
    if (re.findFirstIn(charSeq).getOrElse("") != "") {
      offset += 1
      w ::= c
      if (w.size == size) {
        val value = w.reverse
        w = w.take(size - 1)
        return Some(new MutableNode(value.mkString, "").addOffset(offset))
      }
    }

    None
  }
}

protected[ver3] class GenomeIngestionMapper extends Mapper[LongWritable, Text, Text, MutableNode] with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var started: Boolean = false
  var stub: String = ""
  var winSize: Int = _
  var window: Window = _
  val nmHolder = new StringBuilder
  val lineHolder = new StringBuilder
  var flName = ""

  protected override def map(lnNumber: LongWritable, line: Text, context: Mapper[LongWritable, Text, Text, MutableNode]#Context): Unit = {
    //Initialize
    if (!started) {
      flName = context.getInputSplit match {
        case fs: FileSplit => fs.getPath.getName
        case _ => throw new IllegalStateException("Error getting filename from InputSplit")
      }

      winSize = context.getConfiguration.get(WINDOW_SIZE) match {
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
      return
    }

    lineHolder.append(stub).append(line.toUpperCase)
    stub = lineHolder.substring(lineHolder.length - (winSize - 1))

    lineHolder foreach (c =>
      window.slide(c) match {
        case some: Some[MutableNode] =>
          val node = some.get
          node.chrName = flName
          context.write(nmHolder.append(node.name).append(":").append(flName).toString, node)
          nmHolder.clear
        case None =>
      }
      )
    lineHolder.clear
  }
}

protected[ver3] class GenomeIngestionReducer extends Reducer[Text, MutableNode, Text, MutableNode] with HImplicits {
  protected override def reduce(key: Text, value: java.lang.Iterable[MutableNode], context: Reducer[Text, MutableNode, Text, MutableNode]#Context): Unit = {
    val itr = value.iterator()

    val res = itr.next.deepCopy()

    while (itr.hasNext) {
      val next = itr.next.deepCopy()
      res.merge(next)
    }
    context.write(key, res)
  }
}

protected[ver3] class Node(var name: String, var chrName: String) extends Writable {
  var offsets: List[Int] = List()
  var sorted: Boolean = false

  def this() = this ("", "")

  def deepCopy() = {
    val clone = new Node(this.name, this.chrName)
    clone.offsets = this.offsets
    clone
  }

  def getFreq = offsets.size

  def addOffset(o: Int) = {
    offsets = o :: offsets
    sorted = false
    this
  }

  def sort = {
    if (!sorted) {
      offsets = offsets.sorted
      sorted = true
    }
    this
  }

  override def write(out: DataOutput) {
    WritableUtils.writeString(out, this.name + ":" + this.chrName)
    WritableUtils.writeVInt(out, offsets.size)
    offsets.foreach(WritableUtils.writeVInt(out, _))
  }

  override def readFields(in: DataInput) {
    val node = new Node
    val l = WritableUtils.readString(in)

    l.split(":") match {
      case p: Array[String] if p.size == 2 => node.name = p(0); node.chrName = p(1)
      case _ => throw new IllegalArgumentException("Expected name:chName found " + l)
    }

    val size = WritableUtils.readVInt(in)

    for (i <- 0 until size) node.addOffset(WritableUtils.readVInt(in))

    this.name = node.name
    this.chrName = node.chrName
    this.offsets = node.offsets
    this.sort
  }

  override def toString = this.offsets.size + ":" + this.offsets.mkString(",")

  override def equals(o: Any): Boolean = o match {
    case that: Node => (this.name + this.chrName).toLowerCase.equals((that.name + that.chrName).toLowerCase) match {
      case true => sort.offsets.equals(that.sort.offsets)
      case _ => false
    }
    case _ => false
  }

  def merge(that: Node) {
    if (!this.name.equalsIgnoreCase(that.name) || !this.chrName.equalsIgnoreCase(that.chrName)) {
      throw new IllegalArgumentException(String.format("Expected %s %s Found %s %s", this.name, this.chrName, that.name, that.chrName))
    }
    this.sorted = false
    this.offsets = this.offsets ::: that.offsets
    //this.offsets = this.offsets.distinct
  }
}


protected[ver3] class MutableNode(var name: String, var chrName: String) extends Writable {
  var offsets = new ArrayList[Int]()

  def this() = this ("", "")

  def deepCopy() = {
    val clone = new MutableNode(this.name, this.chrName)
    clone.offsets = new ArrayList(this.offsets)
    clone
  }

  def getFreq = offsets.size

  def addOffset(o: Int) = {
    offsets.add(o)
    this
  }

  override def write(out: DataOutput) {
    WritableUtils.writeString(out, this.name)
    WritableUtils.writeString(out, this.chrName)
    WritableUtils.writeVInt(out, offsets.size)
    offsets.foreach(WritableUtils.writeVInt(out, _))
  }

  override def readFields(in: DataInput) {
    val node = new MutableNode
    node.name = WritableUtils.readString(in)
    node.chrName = WritableUtils.readString(in)
    val size = WritableUtils.readVInt(in)

    for (i <- 0 until size) node.addOffset(WritableUtils.readVInt(in))

    this.name = node.name
    this.chrName = node.chrName
    this.offsets = node.offsets
  }

  override def toString = this.offsets.size + ":" + this.offsets.sorted.mkString(",")

  //  override def equals(o: Any): Boolean = o match {
  //    case that: MutableNode => (this.name + this.chrName).toLowerCase.equals((that.name + that.chrName).toLowerCase) match {
  //      case true => offsets.sorted.equals(that.offsets.sorted)
  //      case _ => false
  //    }
  //    case _ => false
  //  }

  def merge(that: MutableNode) {
    if (!this.name.equalsIgnoreCase(that.name) || !this.chrName.equalsIgnoreCase(that.chrName)) {
      throw new IllegalArgumentException(String.format("Expected %s %s Found %s %s", this.name, this.chrName, that.name, that.chrName))
    }
    that.offsets.foreach(this.offsets.add(_))
  }
}
