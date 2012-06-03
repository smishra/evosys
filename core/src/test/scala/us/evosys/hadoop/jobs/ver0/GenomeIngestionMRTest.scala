package us.evosys.hadoop.jobs.ver0

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

class GenomeIngestionMRTest extends HImplicits {
  val logger: Logger = LoggerFactory.getLogger(classOf[GenomeIngestionMapper])

  @Test
  def map {
    val mapper: Mapper[LongWritable, Text, Text, NLink] = new GenomeIngestionMapper

    val mapDriver = new MapDriver[LongWritable, Text, Text, NLink]
    mapDriver.getConfiguration.set(GenomeIngestionMapper.WINDOW_SIZE, 3 + "")
    mapDriver.setMapper(mapper)

    val flName = "somefile"
    mapDriver.withInput(1, "AAbATnG\nCCA")
    mapDriver.withOutput("AAA", NLink("AAA", flName).add(new NTerm("AAA", flName, 0))).
      withOutput("AAT", NLink("AAT", flName).add(new NTerm("AAT", flName, 1))).
      withOutput("ATG", NLink("ATG", flName).add(new NTerm("ATG", flName, 2))).
      withOutput("TGC", NLink("TGC", flName).add(new NTerm("TGC", flName, 3))).
      withOutput("GCC", NLink("GCC", flName).add(new NTerm("GCC", flName, 4))).
      withOutput("CCA", NLink("CCA", flName).add(new NTerm("CCA", flName, 5)))

    val res = mapDriver.run()
    logger.debug(res.toString)
  }

  @Test
  def reduce {
    val reducer: Reducer[Text, NLink, Text, NLink] = new GenomeIngestionReducer

    val reduceDriver = new ReduceDriver[Text, NLink, Text, NLink]
    reduceDriver.setReducer(reducer)

    reduceDriver.withInput("aaa:ch1",
      List(NLink("AAA", "ch1").add(new NTerm("aaa", "ch1", 0)),
        NLink("aaa", "ch1").add(new NTerm("aaa", "ch1", 2044))))

    val s = "2:0,2044"

    val result = reduceDriver.run().head.getSecond
    Assert.assertTrue("aaa".equalsIgnoreCase(result.name) && "ch1".equals(result.chrName) && 2 == result.getFreq)

    Assert.assertEquals(s, result.toString)
  }

  @Test
  def mapReduce {
    val mapper: Mapper[LongWritable, Text, Text, NLink] = new GenomeIngestionMapper
    val reducer: Reducer[Text, NLink, Text, NLink] = new GenomeIngestionReducer

    val mrDriver = new MapReduceDriver[LongWritable, Text, Text, NLink, Text, NLink]
    mrDriver.getConfiguration.set(GenomeIngestionMapper.WINDOW_SIZE, 3 + "")

    mrDriver.setMapper(mapper)
    mrDriver.setReducer(reducer)

    //mrDriver.withInput(1, "AAATG\nCCAAA")
    mrDriver.withInput(1, "AABAAHTCAAaNT")

    mrDriver.run().foreach(p => {
      val l: NLink = p.getSecond
      logger.debug(l.toString)
      l.name match {
        case "AAA" =>
          Assert.assertEquals(3, l.getFreq)
          Assert.assertEquals(List(0, 1, 6), l.locations)
        case "AAT" =>
          Assert.assertEquals(2, l.getFreq)
          Assert.assertEquals(List(2, 7), l.locations)
        case _ => Assert.assertEquals(1, l.getFreq)
      }
    }
    )
  }
}

