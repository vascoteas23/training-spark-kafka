import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
  val sc = new SparkContext(conf)

  // Create RDD from a Scala collection of multiple words and do a
  // word count ordenado de forma alfabética o resultado das palavras

  val collection = List("ola eu sou o João", "uma frase gira", "duas frases o a giras", "ldsladdwe")

  val rdd = sc.parallelize(collection)

  val words = rdd.flatMap(ch => ch.split(" "))

  val count = words.map(a => (a,1))

  val result = count.reduceByKey((a, b) => a + b)
  //val result = rdd.flatMap(ch => ch.split(" ")).collect().sortWith(_<_)

  val sorted_scala = words.collect().sortWith(_<_)

  val sorted_spark = result.sortByKey(false)

  println(count)
  //result.foreach(println)

  sorted_scala.foreach(println)
  println()
  sorted_spark.foreach(print)
}
