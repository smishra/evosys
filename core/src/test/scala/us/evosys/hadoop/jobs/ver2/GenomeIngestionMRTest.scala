package us.evosys.hadoop.jobs.ver2

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
      val mapper: Mapper[LongWritable, Text, Text, Node] = new GenomeIngestionMapper

      val mapDriver = new MapDriver[LongWritable, Text, Text, Node]
      mapDriver.getConfiguration.set(WINDOW_SIZE, 3 + "")
      mapDriver.setMapper(mapper)

      val flName = "somefile"
      mapDriver.withInput(1, "AAbAATnG\nCCA")

      mapDriver.withOutput("AAA", new Node("AAA", flName, 0)).
        withOutput("AAA", new Node("AAA", flName, 1)).
        withOutput("AAT", new Node("AAA", flName, 2)).
        withOutput("ATG", new Node("ATG", flName, 3)).
        withOutput("TGC", new Node("TGC", flName, 4)).
        withOutput("GCC", new Node("GCC", flName, 5)).
        withOutput("CCA", new Node("CCA", flName, 6))

      logger.info("map result: " + mapDriver.run())
    }

    @Test
    def reduce {
      val reducer: Reducer[Text, Node, Text, Node] = new GenomeIngestionReducer

      val reduceDriver = new ReduceDriver[Text, Node, Text, Node]
      reduceDriver.setReducer(reducer)

      reduceDriver.withInput("aaa", List[Node](new Node("aaa", "ch1", 0),
        new Node("aaa", "ch1", 2044),
        new Node("aaa", "ch1", 3044)))

      val s = "AAA:3:[ch1->0,2044;ch2->3044"

      val result = reduceDriver.run().head.getSecond
      Assert.assertEquals(3, result.getFreq)

      val itr = result.iterator

      Assert.assertEquals(0, itr.next.offset)

      Assert.assertEquals(2044, itr.next.offset)

      Assert.assertEquals(3044, itr.next.offset)
    }

    @Test
    def mapReduce {
      val mapper: Mapper[LongWritable, Text, Text, Node] = new GenomeIngestionMapper
      val reducer: Reducer[Text, Node, Text, Node] = new GenomeIngestionReducer

      val mrDriver = new MapReduceDriver[LongWritable, Text, Text, Node, Text, Node]
      mrDriver.getConfiguration.set(WINDOW_SIZE, 3 + "")

      mrDriver.setMapper(mapper)
      mrDriver.setReducer(reducer)

      //mrDriver.withInput(1, "AAATG\nCCAAA")
      mrDriver.withInput(1, "AABAAHTCAAaNtc")

      mrDriver.run().foreach(p =>
      {
        logger.debug(p.getSecond.toString)
          val node = p.getSecond
          node.name.toUpperCase match {
          case "AAA" => Assert.assertEquals(3, node.getFreq)
          case "ATC" => Assert.assertEquals(2, node.getFreq)
          case "AAT" => Assert.assertEquals(2, node.getFreq)
          case _ => Assert.assertEquals(1, node.getFreq)
        }
      })
    }
}

