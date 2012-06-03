package us.evosys.hadoop.jobs.ver1

import org.slf4j.{Logger, LoggerFactory}
import org.junit.{Assert, Test}
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.JavaConversions._
import org.apache.hadoop.mrunit.mapreduce.{MapReduceDriver, ReduceDriver, MapDriver}
import us.evosys.hadoop.jobs.HImplicits


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/19/12
 * Time: 1:19 PM
 * To change this template use File | Settings | File Templates.
 */

class GenomeIngestionMRTest extends HImplicits with GImplicits {
  val logger: Logger = LoggerFactory.getLogger(classOf[GenomeIngestionMapper])

  @Test
  def map {
    val mapper: Mapper[LongWritable, Text, Text, NTermWritable] = new GenomeIngestionMapper

    val mapDriver = new MapDriver[LongWritable, Text, Text, NTermWritable]
    mapDriver.getConfiguration.set(GenomeIngestionMapper.WINDOW_SIZE, 3 + "")
    mapDriver.setMapper(mapper)

    val flName = "somefile"
    mapDriver.withInput(1, "AAbATnG\nCCA")

    mapDriver.withOutput("AAA", new NTerm("AAA", 1, flName, 0)).
      withOutput("AAT", new NTerm("AAT", 1, flName, 1)).
      withOutput("ATG", new NTerm("ATG", 1, flName, 2)).
      withOutput("TGC", new NTerm("TGC", 1, flName, 3)).
      withOutput("GCC", new NTerm("GCC", 1, flName, 4)).
      withOutput("CCA", new NTerm("CCA", 1, flName, 5))

    logger.info("map result: " + mapDriver.run())
  }

  @Test
  def reduce {
    val reducer: Reducer[Text, NTermWritable, Text, NLink] = new GenomeIngestionReducer

    val reduceDriver = new ReduceDriver[Text, NTermWritable, Text, NLink]
    reduceDriver.setReducer(reducer)

    reduceDriver.withInput("aaa", List[NTermWritable](new NTerm("aaa", 1, "ch1", 0),
      new NTerm("aaa", 1, "ch1", 2044),
      new NTerm("aaa", 1, "ch2", 3044)))

    val s = "AAA:3:[ch1->0,2044;ch2->3044"

    val result = reduceDriver.run().head.getSecond
    logger.debug("reduce result: " + result)

    Assert.assertEquals(s, result.toString)
  }

  @Test
  def mapReduce {
    val mapper: Mapper[LongWritable, Text, Text, NTermWritable] = new GenomeIngestionMapper
    val reducer: Reducer[Text, NTermWritable, Text, NLink] = new GenomeIngestionReducer

    val mrDriver = new MapReduceDriver[LongWritable, Text, Text, NTermWritable, Text, NLink]
    mrDriver.getConfiguration.set(GenomeIngestionMapper.WINDOW_SIZE, 3 + "")

    mrDriver.setMapper(mapper)
    mrDriver.setReducer(reducer)

    //mrDriver.withInput(1, "AAATG\nCCAAA")
    mrDriver.withInput(1, "AABAAHTCAAaN")

    mrDriver.run().foreach(p => logger.debug(p.getSecond.toString))
  }
}

