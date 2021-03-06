package us.evosys.hadoop.jobs.ver1

import org.junit.{Assert, Test}


/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/18/12
 * Time: 3:32 PM
 * To change this template use File | Settings | File Templates.
 */

class TermWindowLinkTest extends GImplicits {
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
    val nterm1 = new NTerm("AAA", 2021, "ch1", 2012)
    val nterm5 = new NTerm("aaa", 2021, "ch1", 2012)

    val nterm2 = new NTerm("AAA", 3021)
    val nterm3 = new NTerm("aaa", 2031, "ch3", 2012)
    val nterm4 = new NTerm("AAT", 2021, "ch1", 2012)

    Assert.assertEquals(nterm1, nterm5)
    Assert.assertFalse(nterm1 equals nterm2)
    Assert.assertFalse(nterm1 equals nterm3)
    Assert.assertFalse(nterm1 equals nterm4)
  }

  @Test
  def combinedNTerm {
    val nterm1 = new NTerm("AAA", 2021, "ch1", 2012)

    val nterm2 = new NTerm("AAA", 3021)
    val nterm3 = new NTerm("aaa", 2031, "ch3", 2012)
    val nterm4 = new NTerm("AAT", 2021, "ch1", 2012)

    val combNTerm = new NTermWritable("AAA")
    combNTerm.add(nterm1).add(nterm2).add(nterm3)

    try {
      combNTerm.add(nterm4)
      Assert.fail("Exception should be thrown")
    }
    catch {
      case ex: IllegalArgumentException =>
    }

    Assert.assertEquals(3, combNTerm.termList.size)
  }

  //@Test
  def nlink {
    val nlink = new NLink("aAa")

    val nterm1 = new NTerm("AAA", 2021, "ch1", 2012)
    val nterm2 = new NTerm("AAA", 3021, "ch1", 3020)
    val nterm3 = new NTerm("aaa", 2031, "ch3", 2012)
    val nterm4 = new NTerm("AAT", 2021, "ch1", 2012)

    val freq = nterm1.freq + nterm2.freq + nterm3.freq

    nlink.add(nterm1)
    nlink.add(nterm2)
    nlink.add(nterm3)
    try {
      nlink.add(nterm4)
      Assert.fail("IlligalArgumentException should be thrown")
    }
    catch {
      case ex: IllegalArgumentException =>
    }

    Assert.assertEquals(freq, nlink.getFreq)

    Assert.assertEquals(2, nlink.getLocations.size)

    Assert.assertEquals(2, nlink.getLocations.get("ch1").get.size)
    Assert.assertEquals(1, nlink.getLocations.get("ch3").get.size)

    Assert.assertTrue(nlink.getLocations.get("ch1").get.contains(2012))
    Assert.assertTrue(nlink.getLocations.get("ch1").get.contains(3020))
    Assert.assertTrue(nlink.getLocations.get("ch3").get.contains(2012))
  }


  @Test
  def nlinkMkString {
    val nlink = new NLink("aAa")

    val nterm1 = new NTerm("AAA", 2021, "ch1", 2012)
    val nterm2 = new NTerm("AAA", 3021, "ch1", 3020)
    val nterm3 = new NTerm("aaa", 2031, "ch3", 2012)

    val freq = nterm1.freq + nterm2.freq + nterm3.freq

    nlink.add(nterm1)
    nlink.add(nterm2)
    nlink.add(nterm3)

    val s = nlink.toString
    System.out.println("NLink.mkString: " + s)

    val res = NLink.fromString(s)
    System.out.println("result: " + res )
    Assert.assertEquals(s, res.toString)
  }
}