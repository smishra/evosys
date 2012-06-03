package us.evosys.hadoop.jobs

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


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/4/12
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */
class NonSplitableInputFormat extends TextInputFormat {
  override protected def isSplitable(ctx: JobContext, fileName: Path): Boolean = false
}



//class MultiFileOutput extends MultipleTextOutputFormat[]























