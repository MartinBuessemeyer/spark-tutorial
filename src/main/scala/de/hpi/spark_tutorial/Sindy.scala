package de.hpi.spark_tutorial

// https://sparkbyexamples.com/spark/spark-collect-list-and-collect-set-functions/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    val datasets = inputs.map(spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(_))
    //datasets.foreach(d => println(d.columns.mkString("Array(", ", ", ")")))

    // TODO maybe improve getAs

    val cells = datasets.map(
      dataset => {
        val cols = dataset.columns
        dataset.flatMap(row => {
          val rowSeq = row.toSeq.asInstanceOf[Seq[String]]
          (0 until row.length).
            map(colIdx =>
              (rowSeq(colIdx), cols(colIdx))
            )
        }).toDF("val", "col")
      }).reduce(_.union(_))
    // https://stackoverflow.com/a/38063180
    val attributeSets = cells
      .groupBy("val")
      .agg(collect_set("col"))
      .select("collect_set(col)")
      .withColumnRenamed("collect_set(col)", "attribute_sets")
      .distinct().toDF("attribute_sets")
    /*val INDs = attributeSets
      .flatMap(attributeSet =>
        //attributeSet.toSeq.combinations()
        attributeSet.toSeq.map(attr => (attr, attributeSet - Set(attr))))*/
    // https://stackoverflow.com/a/44437064
    val inclusion_lists = attributeSets.withColumn("key_attr", explode(col("attribute_sets")))
      // delete the attribute from the attribute list.
      .map(temp =>
        (temp.getString(1),
          temp.getSeq(0).asInstanceOf[Seq[String]].
            filter(!_.equals(
              temp.getString(1)))))
      // to dataset https://stackoverflow.com/a/44516773
      //.toDF("key_attr", "attribute_sets")
      .withColumnRenamed("_1", "key_attr")
      .withColumnRenamed("_2", "attribute_sets")
      .as[(String, Seq[String])]

    // No reduceByKey :(
    val aggregations = inclusion_lists
      .groupByKey(_._1)
      /*.mapValues(key_and_attribute_sets_collection =>
        key_and_attribute_sets_collection._2.reduce(_.intersect(_))
      ).*/
      // LongestCommonSubstring lines 70ff.
      .mapGroups((attr_key, attribute_sets_collection) =>
        (attr_key, attribute_sets_collection.map(_._2).reduce(_.intersect(_))))
    val IDNs = aggregations.filter(_._2.nonEmpty).sort(asc("_1"))
    // https://sparkbyexamples.com/spark/print-the-contents-of-rdd-in-spark-pyspark/
    // looks like everything is printed to the console even when distributed after the collect call.
    IDNs.collect().foreach(idn =>
    println(idn._1 + " < " + idn._2.sorted.reduce(_ + ", " + _)))
  }
}
