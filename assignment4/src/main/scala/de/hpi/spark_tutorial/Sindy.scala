package de.hpi.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession, partitionCount :Int = 32): Unit = {

    val tables = inputs.map { source =>
      spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(source)
        .repartition(partitionCount)
    }

    //tables.foreach(_.printSchema())
    //tables.foreach(_.show())

    var rddBuffer = new ListBuffer[RDD[(String, Set[String])]]

    // create a buffer containing RDDs for each column of all tables with each element having the (<value>, <columnname>) format
    tables.foreach(table => table.columns.foreach(col =>
      rddBuffer += table.select(col).dropDuplicates().rdd.map(r => (r(0).toString, Set[String](col)))
    ))

    // see content of first rdd column
    //rddBuffer.toList(0).collect().foreach(println)

    // convert buffer to list
    val rddList = rddBuffer.toList

    // convert List[RDD] to RDD
    val rdd = rddList.reduce(_ union _)
    //rdd.foreach(r => println(r))

    // translates to reduceByKey((a,b) => a ++ b) --> ++ is a method defined on List that concatenates another list to it
    val attributes = rdd.reduceByKey(_ ++ _)
    //attributes.foreach(a => println(a))

    // get the attribute sets just containing the column name
    val attributesColumnNames = attributes.values.distinct()
    //attributesColumnNames.foreach(b => println(b))

    // create inclusions lists
    val inclusionList = attributesColumnNames.flatMap(attributes => attributes.map(a => (a, attributes - a)))
    //inclusionList.foreach(i => println(i))

    // groupby inclusion lists by key and intersect their attribute sets + filter out empty sets as values + sort lexicographically
    val indList = inclusionList.reduceByKey(_ intersect _).filter(x => x._2.nonEmpty).sortByKey()
    //indList.foreach(i => println(i))

    // output resulting strings
    val output = indList.collect()
    output.foreach(i => println(i._1 + " < " + i._2.mkString(", ")))
  }
}
