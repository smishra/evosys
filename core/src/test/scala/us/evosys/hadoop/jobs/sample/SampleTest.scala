package us.evosys.hadoop.jobs.sample

import org.junit.Test
import util.matching.Regex
import java.io._

/**
 * Created by IntelliJ IDEA.
 * User: smishra
 * Date: 1/11/12
 * Time: 5:43 PM
 * To change this template use File | Settings | File Templates.
 */

class SampleTest {
  @Test
  def run = {
    val map: Map[String, List[Int]] = Map("ch1" -> List(1, 11, 111), "ch2" -> List(2, 22, 222), "ch3" -> List(3, 33, 333))
    System.out.println(map.mkString("freq:" + 5 + ":", ":", ""))

    var s: String = "aaa" + ":" + 50 + ":["

    map.foreach(t => {
      s = s + t._1 + "->" + t._2.mkString(",") + ";"
    })

    s = s.take(s.size - 1)

    System.out.println("for each result: " + s)

    s split (":") match {
      case a: Array[String] if (a.size == 3) => {
        System.out.println("name: " + a(0))
        System.out.println("freq: " + a(1))

        var locations: Map[String, List[Int]] = Map()

        a(2).tail match {
          case s: String => s.split(";") foreach {
            t => t.split("->") match {
              case e: Array[String] if (e.size == 2) =>
                val intArr: Array[Int] = e(1).split(",") match {
                  case strArr: Array[String] => for (s <- strArr) yield s.toInt
                  case _ => throw new IllegalArgumentException("unexpected " + e(1))
                }

                locations += (e(0) -> intArr.toList)
            }
          }
        }
        System.out.println("locations: " + locations)
      }
      case _ => throw new IllegalArgumentException("parse error: " + s)
    }
  }

  @Test
  def pattern {
    val str:String = "AAANTG1CTTB\nGC"
    val re:Regex = "(?i)A|T|G|C".r

    val filtered = for (c <- str if (re.findFirstIn(c.toString).getOrElse("")!="") )
    yield {c}

    System.out.println("filtered: " + filtered)
  }

  @Test
  def forTest {
    val list:List[Int] = List(1)

    for (i <- 0 until list.size) System.out.println(list(i))
  }
}