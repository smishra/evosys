package us.evosys.hadoop.jobs.ver3

import org.junit.{Assert, Test}


/**
 * Created by IntelliJ IDEA.
 * User: sanjeev
 * Date: 1/28/12
 * Time: 8:17 AM
 * To change this template use File | Settings | File Templates.
 */

class NodeTest {
    @Test
  def singleChromosome {
    val head = new Node("AAA", "ch1").addOffset(0).addOffset(10).addOffset(2300).addOffset(2304).addOffset(3105).addOffset(35000).addOffset(36000)

    head.offsets.foreach(System.out.println(_))
    Assert.assertEquals(7, head.offsets.size)
    Assert.assertEquals(7, head.getFreq)
  }

  @Test
  def compare {
    val head = new Node("AAA", "ch1").addOffset(0)
    val n0 = new Node("aaa", "ch1").addOffset(0)

    val n1 = new Node("AAA", "ch1").addOffset(2)

    Assert.assertEquals(head, n0)

    Assert.assertFalse(head equals n1)

    val n2 = new Node("AAT", "ch1").addOffset(2)

    Assert.assertFalse(n1 equals n2)
  }

  @Test
  def sort {
    val head = new Node("AAA", "ch1").addOffset(1101).addOffset(100).addOffset(308).addOffset(304).addOffset(31050).addOffset(36000).addOffset(3600)
    head.offsets.foreach(System.out.println(_))
    System.out.println("After sorting...")
    head.sort.offsets.foreach(System.out.println(_))
  }
}