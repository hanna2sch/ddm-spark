package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.util.matching.Regex

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._
    //A ⊆ B ⇔ ∀t ∈ O : (t[A] != ⊥ ⇒ t[B] != ⊥)
    // DataSource -> FlatMap(split records) -> Reduce by value (union attributes) -> FlatMap (Create inclusion lists) -> Reduce by first attr (intersect attributes) -> Data Sink
    var datasink = inputs
      .map(input => readData(input, spark)) // List[Dataset[Row]]
      .map(ds => ds.flatMap(row => row.schema.names.map(name => (row.getString(row.fieldIndex(name)), Set(name))))) // List[Dataset[(String, Set[String])]]
      .reduce(_.union(_)) // Dataset[(String, Set[String])]
      .groupByKey(x => x._1) // KeyValueGroupedDataset[String, (String, Set[String])]
      .mapValues(x => x._2)
      .reduceGroups(_.union(_)) // Dataset[(String, Set[String])]
      .filter(set => set._2.size > 1)
    var mapping = datasink.map(x => x._2) // Dataset[Set[String]]
      .flatMap(x => x.map(v => (v, x-v))) // Dataset[(String, Set[String])] //permutations
    var aggregate = mapping
      .groupByKey(x => x._1)
      .mapValues(x => x._2)
      .reduceGroups((k, v) => k.intersect(v)) // Dataset[(String, Set[String])]
    var ind = aggregate.filter(x => x._2.size > 1) // Dataset[(String, Set[String])]
    var b = ind.map(entry => (entry._1 + " < " + entry._2.toString().slice(4, entry._2.toString().length-1))).collect().sorted
    b.foreach(println(_))
  }
}
