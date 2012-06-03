package us.evosys.hadoop.jobs

import org.slf4j.LoggerFactory
import org.junit.Test
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import scala.collection.JavaConversions._
import org.apache.hadoop.io.{NullWritable, LongWritable, Text}

/**
 * Created by IntelliJ IDEA.
 * User: sanjeev
 * Date: 2/10/12
 * Time: 10:30 AM
 * To change this template use File | Settings | File Templates.
 */

class ChrFileCleanerTest extends HImplicits {
  val logger = LoggerFactory.getLogger(classOf[ChrFileCleanerTest])

  @Test
  def map {
//    val mapper: Mapper[LongWritable, Text, NullWritable, Text] = new ChrFileCleaningMapper
//    val mapDriver = new MapDriver[LongWritable, Text, NullWritable, Text]
//    mapDriver.setMapper(mapper)
//
//    val flName = "somefile"
//
//    mapDriver.withInput(1, "BAAbANATnG\nCCAN")
//    //
//    //    logger.debug("result {}",mapDriver.run().head.getSecond)
//
//    mapDriver.withOutput(null,">somefile").withOutput(null, "AAAATG\nCCA")
//    mapDriver.runTest()
  }
}