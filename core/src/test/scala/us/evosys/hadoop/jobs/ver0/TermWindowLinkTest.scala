package us.evosys.hadoop.jobs.ver0

import org.junit.{Assert, Test}


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/18/12
 * Time: 3:32 PM
 * To change this template use File | Settings | File Templates.
 */

class TermWindowLinkTest {
  @Test
  def window3 {
    val winSize = 3
    val win = Window(winSize)
    val str: String = "AAATGC\n\rGGC"
    var i = 0
    System.out.println("Expects: AAA AAT ATG TGC GCG CGG GGC")
    while (i < str.length) {
      win.slide(str.charAt(i)) match {
        case nt: Some[NTerm] =>
          if (i + 1 < winSize || Character.isWhitespace(str.charAt(i)))
            Assert.fail("wrong logic: term: " + nt.get.value)
        case None =>
      }
      i += 1
    }
  }

  @Test
  def window4 {
    val winSize = 4
    val win = Window(winSize)
    val str: String = "AABATGC\nGGKC"
    var i = 0
    System.out.println("Expects: AAAT AATG ATGC TGCG GCGG CGGC")
    while (i < str.length) {
      win.slide(str.charAt(i)) match {
        case nt: Some[NTerm] =>
          if (i + 1 < winSize || Character.isWhitespace(str.charAt(i)))
            Assert.fail("wrong logic: term: " + nt.get.value)
        case None =>
      }
      i += 1
    }
  }

  @Test
  def nterm {
    val nterm1 = new NTerm("AAA", "ch1", 2012)
    val nterm5 = new NTerm("aaa", "ch1", 2012)

    val nterm2 = new NTerm("AAA", 3021)
    val nterm3 = new NTerm("aaa", "ch3", 2012)
    val nterm4 = new NTerm("AAT", "ch1", 2012)

    Assert.assertEquals(nterm1, nterm5)
    Assert.assertFalse(nterm1 equals nterm2)
    Assert.assertFalse(nterm1 equals nterm3)
    Assert.assertFalse(nterm1 equals nterm4)
  }

  @Test
  def nlink {
    val nlink = new NLink("aAa", "CH1")

    val nterm1 = new NTerm("AAA", "ch1", 2012)
    val nterm2 = new NTerm("AAA", "ch1", 3020)
    val nterm3 = new NTerm("aaa", "ch3", 2012)
    val nterm4 = new NTerm("AAT", "ch1", 2012)

    nlink.add(nterm1)
    nlink.add(nterm2)
    try {
      nlink.add(nterm3)
      Assert.fail("IlligalArgumentException should be thrown")
    }
    catch {
      case ex: IllegalArgumentException =>
    }
    try {
      nlink.add(nterm4)
      Assert.fail("IlligalArgumentException should be thrown")
    }
    catch {
      case ex: IllegalArgumentException =>
    }

    Assert.assertEquals(2, nlink.getFreq)

    Assert.assertEquals(2, nlink.locations.size)
  }
}