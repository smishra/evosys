package us.evosys.hadoop.jobs

import org.apache.hadoop.conf.Configured
import org.slf4j.{LoggerFactory, Logger}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import util.matching.Regex
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputCommitter, TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.io.{SequenceFile, NullWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.util.{ReflectionUtils, Tool, ToolRunner}
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec, GzipCodec}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import java.io.DataOutputStream

/**
 * Created by IntelliJ IDEA.
 * User: sanjeev
 * Date: 2/9/12
 * Time: 11:10 AM
 * To change this template use File | Settings | File Templates.
 */

object ChrFileCleaner extends Configured with Tool with HImplicits {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(args: Array[String]): Int = {
    val conf = getConf
    conf.setQuietMode(false)

    val job: Job = new Job(conf, "ChrFile Cleaner")

    job.setInputFormatClass(classOf[NonSplitableInputFormat])

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[ChrFileCleaningMapper])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setNumReduceTasks(0)
    for (i <- 0 to args.length - 2) {
      logger.info("processing {}: {}", i, args(i))
      FileInputFormat.addInputPath(job, args.apply(i))
    }

    FileOutputFormat.setOutputPath(job, args.last)
    job.setOutputFormatClass(classOf[BinaryOutputFormat[LongWritable, Text]])
    //job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, Text]])
    //FileOutputFormat.setOutputCompressorClass(job, classOf[GzipCodec])
    //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK)

    job.waitForCompletion(true) match {
      case true => 0
      case false => 1
    }
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, args))
  }
}

protected[jobs] class ChrFileCleaningMapper extends Mapper[LongWritable, Text, LongWritable, Text] with HImplicits {
  val lineHolder: StringBuilder = new StringBuilder
  val re: Regex = "(?i)A|T|G|C|\n|\r".r
  val charSeq = new Array[Char](1)
  val out = new StringBuilder
  var started = false
  var flName = ""
  var lnNumOut = 0

  protected override def map(lnNum: LongWritable, line: Text, context: Mapper[LongWritable, Text, LongWritable, Text]#Context) {
    if (!started) {
      flName = context.getInputSplit match {
        case fs: FileSplit => fs.getPath.getName
        case _ => throw new IllegalStateException("flName can't be determined!!!")
      }
      context.write(lnNum, ">" + flName)
      started = true
    }

    //Ignore the non genome related line
    if (line.startsWith(">")) {
      return
    }

    lineHolder.append(line)
    lineHolder.foreach(c => {
      charSeq(0) = c
      re.findFirstIn(charSeq) match {
        case s: Some[String] => out.append(c)
        case None =>
      }
    })
    context.write(1, out.toString)
    out.clear
    lineHolder.clear
  }
}

protected[jobs] class ChrFileSeqOutputFormat[K, V] extends SequenceFileOutputFormat[K, V] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    val (compressionType: CompressionType, codec: CompressionCodec) = if (FileOutputFormat.getCompressOutput(context)) {
      (SequenceFileOutputFormat.getOutputCompressionType(context),
        ReflectionUtils.newInstance(FileOutputFormat.getOutputCompressorClass(context, classOf[DefaultCodec]), context.getConfiguration))
    }
    else {
      (CompressionType.NONE, null)
    }
    val file: Path = getDefaultWorkFile(context, "")
    val fs: FileSystem = file.getFileSystem(context.getConfiguration)

    val out: SequenceFile.Writer =
      SequenceFile.createWriter(fs,
        context.getConfiguration,
        file,
        context.getOutputKeyClass,
        context.getOutputValueClass,
        compressionType,
        codec,
        context)

    new RecordWriter[K, V] {
      def write(key: K, value: V) = out.append(key, value)

      def close(p1: TaskAttemptContext) = out.close
    }
  }
}

protected[jobs] class BinaryOutputFormat[K, V] extends TextOutputFormat[K, V] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    val out: DataOutputStream = if (FileOutputFormat.getCompressOutput(context)) {
      val codec: CompressionCodec = ReflectionUtils.newInstance(FileOutputFormat.getOutputCompressorClass(context, classOf[DefaultCodec]), context.getConfiguration)
      val path: Path = getDefaultWorkFile(context, codec.getDefaultExtension)
      new DataOutputStream(codec.createOutputStream(path.getFileSystem(context.getConfiguration).create(path, false)))
    }
    else {
      val path: Path = getDefaultWorkFile(context, "")
      path.getFileSystem(context.getConfiguration).create(path, false) //out
    }

    new RecordWriter[K, V] {
      def write(key: K, value: V) = out.writeBytes(String.valueOf(value))

      def close(p1: TaskAttemptContext) = out.close
    }
  }
}







