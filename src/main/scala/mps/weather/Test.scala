package mps.weather

object Test {
  def main(args: Array[String]): Unit = {
    val arr = Array(("rain", 10), ("rain", 11), ("cold",12 ), ("freeze",13), ("hot", 14), ("hot", 15))
    val a = arr.map(a => a._1 -> (1, a._2)).groupBy(_._1).mapValues(seq => (seq.map(_._2._1).sum, seq.map(_._2._2).min))
      .toList.sortBy(_._2._1).reverse
    val cnt = a.head._2._1
    val r = a.filter(_._2._1 == cnt).sortBy(_._2._2)
    println(r)

  }
}
