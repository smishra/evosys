package us.evosys.hadoop.jobs

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
    val head = new Node("AAA", "ch1", 0)
    val n0 = new Node("AAA", "ch1", 10)
    val n1 = new Node("AAA", "ch1", 2300)
    val n2 = new Node("AAA", "ch1", 2304)
    val n3 = new Node("AAA", "ch1", 3105)
    val n4 = new Node("AAA", "ch1", 35000)
    val n5 = new Node("AAA", "ch1", 36000)

    head.add(n0).add(n1).add(n2).add(n3).add(n4).add(n5)

    val l = head.asList
    l.foreach(System.out.println(_))
    Assert.assertEquals(7, l.size)
    Assert.assertEquals(7, head.getFreq)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def diffChromosome {
    val head = new Node("AAA", "ch1", 0)
    val n0 = new Node("AAA", "ch2", 10)

    head.add(n0)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def diffName {
    val head = new Node("AAC", "ch1", 0)
    val n0 = new Node("AAA", "ch1", 10)

    head.add(n0)
  }

  @Test
  def iterator {
    val head = new Node("AAA", "ch1", 0)
    val n0 = new Node("AAA", "ch1", 10)
    val n1 = new Node("AAA", "ch1", 2300)
    val n2 = new Node("AAA", "ch1", 2304)
    val n3 = new Node("AAA", "ch1", 3105)
    val n4 = new Node("AAA", "ch1", 35000)
    val n5 = new Node("AAA", "ch1", 36000)

    head.add(n0).add(n1).add(n2).add(n3).add(n4).add(n5)

    val itr1 = head.iterator
    val itr2 = head.iterator

    var counter: Int = 0
    while (itr1.hasNext) {
      itr1.next()
      counter += 1
    }

    Assert.assertEquals(7, counter)

    counter = 0
    while (itr2.hasNext) {
      itr2.next()
      counter += 1
    }

    Assert.assertEquals(7, counter)

    val itr3 = head.iterator
    Assert.assertEquals(head, itr3.next())
    System.out.println(itr3.next())
    System.out.println(itr3.next())

    Assert.assertEquals(n2, itr3.next())
  }

  @Test
  def compare {
    val head = new Node("AAA", "ch1", 0)
    val n0 = new Node("aaa", "ch1", 0)

    val n1 = new Node("AAA", "ch1", 2)

    Assert.assertEquals(0, head.compare(n0))

    Assert.assertTrue(head<n1)

    val n2 = new Node("AAT", "ch1", 2)

    Assert.assertTrue(n1<n2)

    val n3 = new Node("AAA", "ch0", 2)

    Assert.assertTrue(n1>n3)
  }

  @Test
  def sort {
    val head = new Node("AAA", "ch1", 1101)
    val n0 = new Node("AAA", "ch1", 100)
    val n1 = new Node("AAA", "ch1", 308)
    val n2 = new Node("AAA", "ch1", 304)
    val n3 = new Node("AAA", "ch1", 31050)
    val n4 = new Node("AAA", "ch1", 360000)
    val n5 = new Node("AAA", "ch1", 3600)

    head.add(n0).add(n1).add(n2).add(n3).add(n4).add(n5)
    head.toList.foreach(System.out.println(_))
    System.out.println("After sorting...")
    Node.sort(head).toList.foreach(System.out.println(_))
  }
}