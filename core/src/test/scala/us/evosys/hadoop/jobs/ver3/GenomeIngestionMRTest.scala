package us.evosys.hadoop.jobs.ver3

import org.slf4j.{Logger, LoggerFactory}
import org.junit.{Assert, Test}
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.hadoop.io.{LongWritable, Text}
import scala.collection.JavaConversions._
import org.apache.hadoop.mrunit.mapreduce.{MapReduceDriver, ReduceDriver, MapDriver}
import us.evosys.hadoop.jobs.{HImplicits}
import us.evosys.hadoop.jobs.ver1.GenomeIngestionMapper.WINDOW_SIZE


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
    val mapper: Mapper[LongWritable, Text, Text, MutableNode] = new GenomeIngestionMapper

    val mapDriver = new MapDriver[LongWritable, Text, Text, MutableNode]
    mapDriver.getConfiguration.set(WINDOW_SIZE, 3 + "")
    mapDriver.setMapper(mapper)

    val flName = "somefile"
    mapDriver.withInput(1, "AAbAATnG\nCCA")

    mapDriver.withOutput("AAA", new MutableNode("AAA", flName).addOffset(0)).
      withOutput("AAA", new MutableNode("AAA", flName).addOffset(1)).
      withOutput("AAT", new MutableNode("AAA", flName).addOffset(2)).
      withOutput("ATG", new MutableNode("ATG", flName).addOffset(3)).
      withOutput("TGC", new MutableNode("TGC", flName).addOffset(4)).
      withOutput("GCC", new MutableNode("GCC", flName).addOffset(5)).
      withOutput("CCA", new MutableNode("CCA", flName).addOffset(6))

    logger.info("map result: " + mapDriver.run())
  }

  @Test
  def reduce {
    val reducer: Reducer[Text, MutableNode, Text, MutableNode] = new GenomeIngestionReducer

    val reduceDriver = new ReduceDriver[Text, MutableNode, Text, MutableNode]
    reduceDriver.setReducer(reducer)

    reduceDriver.withInput("aaa", List[MutableNode](new MutableNode("aaa", "ch1").addOffset(0),
      new MutableNode("aaa", "ch1").addOffset(2044),
      new MutableNode("aaa", "ch1").addOffset(3044)))

    val result = reduceDriver.run().head.getSecond
    Assert.assertEquals(3, result.getFreq)

    val itr = result.offsets.sorted.iterator

    Assert.assertEquals(0, itr.next)

    Assert.assertEquals(2044, itr.next)

    Assert.assertEquals(3044, itr.next)
  }

  @Test
  def mapReduce {
    val mapper: Mapper[LongWritable, Text, Text, MutableNode] = new GenomeIngestionMapper
    val reducer: Reducer[Text, MutableNode, Text, MutableNode] = new GenomeIngestionReducer

    val mrDriver = new MapReduceDriver[LongWritable, Text, Text, MutableNode, Text, MutableNode]
    mrDriver.getConfiguration.set(WINDOW_SIZE, 3 + "")

    mrDriver.setMapper(mapper)
    mrDriver.setReducer(reducer)

    //mrDriver.withInput(1, "AAATG\nCCAAA")
    mrDriver.withInput(1, "AABAAHTCAAaNtc")

    mrDriver.run().foreach(p => {
      logger.debug(p.getSecond.toString)
      val node = p.getSecond
      node.name.toUpperCase match {
        case "AAA" =>
          Assert.assertEquals(3, node.getFreq)
          Assert.assertEquals(List(0, 1, 6), node.offsets.toList)
        case "AAT" =>
          Assert.assertEquals(2, node.getFreq)
          Assert.assertEquals(List(2, 7), node.offsets.toList)
        case "ATC" =>
          Assert.assertEquals(2, node.getFreq)
          Assert.assertEquals(List(3, 8), node.offsets.toList)
        case _ => Assert.assertEquals(1, node.getFreq)
      }
    })
  }

}

