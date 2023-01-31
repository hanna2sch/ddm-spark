package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

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
    //val data: List[Dataset[(String, Set[String])]] = inputs.map((entry => readData(entry, spark)))
    //println(data.length)
    //val tempdata = data.map()
      //.reduce(_ union _).collect().foreach(x => println(x))
    var data = inputs
      .map(input => readData(input, spark))
     // .map(entry => entry.map(row => row.toSeq.map(x => (x.toString, entry.columns.indices.zipWithIndex.filter(_)))) //List[Dataset[Row]]
    var tuples = data.map(ds => ds.flatMap(row => row.schema.names.map( name => (row.getString(row.fieldIndex(name)), name))))
    //println(tuples(0).collect()(1)(0).toString())
    //println(data(0))
    //var temp = data(0)
    //var temp2 = data(0).collect()
    //println(temp.schema.names(0))
    //println(temp)
    //println(temp2(0).toString)
    var temp = tuples.reduce(_.union(_)) // Dataset[(String, String)]]
    //println(temp)
    var key_value_data = temp.groupByKey(x => x._1) // KeyValueGroupedDataset[String, String]
      //.mapValues(x => x._2)
    var mapping = key_value_data.mapGroups((k, i) => i.map(x => x._2).toSet)
    var filtering = mapping // ^= attributeset
      //.map(x => x.map(y => (y, x.filter(_ != y))))
      .flatMap((x => x.map(y => (y, x.filter(_ != y))))) //^= linclusion list
    //var partition = filtering.repartition(32)
    //var aggregate = filtering.map(x => x._2)
    var aggregate = filtering
      .groupByKey(x => x._1) // KeyValueGroupedDataset[String, (String, Set[String])]
      .mapValues(x => x._2) // KeyValueGroupedDataset[String, Set[String]]
      .reduceGroups((k, v) => k.intersect(v)) // Dataset[(String, Set[String])]
    var ind = aggregate.filter(x => x._2.size > 1) // Dataset[(String, Set[String])]
    var b = ind.map(entry => (entry._1 + " < " + entry._2).toString()).collect().sorted
    b.foreach(println(_))
  }
}
