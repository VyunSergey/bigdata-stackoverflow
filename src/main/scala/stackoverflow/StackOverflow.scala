package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.util.Try

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(
                    postingType: Int,
                    id: Int,
                    acceptedAnswer: Option[Int],
                    parentId: Option[QID],
                    score: Int,
                    tags: Option[String]
                  ) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {
    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored).cache
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  // Parsing utilities:
  def stringToOptionInt(str: String): Option[Int] = Try(str.toInt).toOption

  def stringToInt(str: String): Int = stringToOptionInt(str).getOrElse(0)

  def lineToPosting(line: String): Posting = {
    val arr: Array[String] = line.split(",")

    Posting(
      postingType    = stringToInt(arr(0)),
      id             = stringToInt(arr(1)),
      acceptedAnswer = stringToOptionInt(arr(2)),
      parentId       = stringToOptionInt(arr(3)),
      score          = stringToInt(arr(4)),
      tags           = if (arr.length >= 6) Some(arr(5).intern) else None
    )
  }

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] = {
    lines.map(lineToPosting)
  }

  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions: RDD[(QID, Question)] = postings
      .filter(_.postingType == 1)
      .map(v => (v.id, v))

    val answers: RDD[(QID, Iterable[(QID, Answer)])] = postings
      .filter(_.postingType == 2)
      .filter(_.parentId.nonEmpty)
      .map(v => (v.parentId.get, v))
      .groupBy(_._1)

    questions.join(answers).mapValues(vs => vs._2.map(v => (vs._1, v._2)))
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
    @tailrec
    def answerHighScore(answers: Iterable[Answer], score: HighScore): HighScore = {
      if (answers.isEmpty) {
        score
      } else {
        answerHighScore(answers.tail,
          if (answers.head.score > score) answers.head.score else score
        )
      }
    }

    grouped.map(vs => (vs._2.head._1, answerHighScore(vs._2.map(_._2), 0)))
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    @tailrec
    def firstLangInTag(tag: Option[String], ls: List[String], index: Int): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(index)
      else {
        firstLangInTag(tag, ls.tail, index + 1)
      }
    }

    scored
      .filter(_._1.tags.nonEmpty)
      .map(vs => (firstLangInTag(vs._1.tags, langs, 0).get * langSpread, vs._2))
  }

  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0,
      "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(withReplacement = false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect

    assert(res.length == kmeansKernels, res.length)
    res
  }

  //  Kmeans method:

  /** Main kmeans computation */
  @tailrec
  final def kmeans(means: Array[(Int, Int)],
                   vectors: RDD[(Int, Int)],
                   iter: Int = 1,
                   debug: Boolean = false): Array[(Int, Int)] = {
    val newAverages =
      vectors
      .map(v => (findClosest(v, means), v))
      .groupBy(_._1)
      .mapValues(vs => averageVectors(vs.map(_._2)))
      .collect
      .toMap

    // you need to compute newMeans
    val newMeans = means.indices.map(i => newAverages.getOrElse(i, means(i))).toArray

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }

  //  Kmeans utilities:

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean = {
    distance < kmeansEta
  }

  def squaredDiff(v1: Int, v2: Int): Double = {
    (v1 - v2).toDouble * (v1 - v2)
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    squaredDiff(v1._1, v2._1) + squaredDiff(v1._2, v2._2)
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)

    a1.zip(a2).map(vs => euclideanDistance(vs._1, vs._2)).sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    @tailrec
    def findClosestIndex(p: (Int, Int), centers: Array[(Int, Int)],
                         dist: Double, index: Int, bestIndex: Int): Int = {
      if (centers.isEmpty) {
        bestIndex
      } else {
        val tempDist = euclideanDistance(p, centers.head)
        val (newDist, newIndex) = if (tempDist < dist) (tempDist, index) else (dist, bestIndex)
        findClosestIndex(p, centers.tail, newDist, index + 1, newIndex)
      }
    }
    findClosestIndex(p, centers, Double.PositiveInfinity, 0, 0)
  }

  def sumTuple3(v1: (Double, Double, Double), v2: (Double, Double, Double)): (Double, Double, Double) = {
    (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val (comp1, comp2, count) = ps.map(vs => (vs._1.toDouble, vs._2.toDouble, 1D)).reduce(sumTuple3)

    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def mostValue(arr: Array[Int]): (Int, Double) = {
    val (a, (_, d)) =
      arr.groupBy(a => a)
      .mapValues(v => (v.length, v.length / arr.length * 100.0))
      .maxBy(_._2._1)

    (a, d)
  }

  def medianValue(arr: Array[Int]): Int = {
    val sorted = arr.sorted

    sorted.length match {
      case l if l % 2 == 1 => sorted(l / 2)
      case l if l % 2 == 0 => (sorted((l - 1) / 2) + sorted(l / 2)) / 2
    }
  }

  //  Displaying results:
  def clusterResults(means: Array[(Int, Int)],
                     vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest: RDD[(LangIndex, (LangIndex, HighScore))] =
      vectors.map(p => (findClosest(p, means), p))

    val closestGrouped: RDD[(LangIndex, Iterable[(LangIndex, HighScore)])] =
      closest.groupByKey

    val median = closestGrouped.mapValues { vs =>
      // most common language in the cluster
      val langLabel: String   = langs(mostValue(vs.map(_._1 / langSpread).toArray)._1)
      // percent of the questions in the most common language
      val langPercent: Double = mostValue(vs.map(_._1 / langSpread).toArray)._2
      val clusterSize: Int    = vs.size
      val medianScore: Int    = medianValue(vs.map(_._2).toArray)

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect.map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"$score%7d  $lang%-17s ($percent%-5.1f%%)      $size%7d")
  }
}
