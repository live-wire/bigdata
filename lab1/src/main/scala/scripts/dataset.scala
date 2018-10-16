
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import java.io._
import scala.math.pow
import org.apache.spark.sql.functions._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{alias, collect_list}

case class GDeltClass (
  DATE: Timestamp,
  AllNames: String
)
val schema = StructType(
  Array(
      StructField("GKGRECORDID",  StringType, nullable=true),
      StructField("DATE", TimestampType,  nullable=true),
      StructField("SourceCollectionIdentifier", IntegerType,  nullable=true),
      StructField("SourceCommonName", StringType, nullable=true),
      StructField("DocumentIdentifier", StringType, nullable=true),
      StructField("Counts", StringType, nullable=true),
      StructField("V2Counts", StringType, nullable=true),
      StructField("Themes", StringType, nullable=true),
      StructField("V2Themes", StringType, nullable=true),
      StructField("Locations",  StringType, nullable=true),
      StructField("V2Locations",  StringType, nullable=true),
      StructField("Persons",  StringType, nullable=true),
      StructField("V2Persons",  StringType, nullable=true),
      StructField("Organizations",  StringType, nullable=true),
      StructField("V2Organizations",  StringType, nullable=true),
      StructField("V2Tone", StringType, nullable=true),
      StructField("Dates",  StringType, nullable=true),
      StructField("GCAM", StringType, nullable=true),
      StructField("SharingImage", StringType, nullable=true),
      StructField("RelatedImages",  StringType, nullable=true),
      StructField("SocialImageEmbeds",  StringType, nullable=true),
      StructField("SocialVideoEmbeds",  StringType, nullable=true),
      StructField("Quotations", StringType, nullable=true),
      StructField("AllNames", StringType, nullable=true), //  23  --->  Using this
      StructField("Amounts",  StringType, nullable=true),
      StructField("TranslationInfo",  StringType, nullable=true),
      StructField("Extras", StringType, nullable=true)
  )
)

case class DateWord(date: String, topic: String)
import spark.implicits._
val ds = spark.read 
              .schema(schema) 
              .option("timestampFormat", "yyyyMMddhhmmss")
              .option("delimiter", "\t")
              .csv("./segment/*.csv")
              .as[GDeltClass]

val byDate = Window.partitionBy('date).orderBy('count desc) //window function
val rankByDate = rank().over(byDate)

val gdelt = ds.filter(line=>line.AllNames!=null && line.DATE!=null)
              .map(t=>(t.DATE.toString().split(" ")(0), t.AllNames.split(";")))
              .map(t=>(t._1,t._2.map(tin=>tin.split(",")(0)).distinct))
              .flatMap(t=>t._2.map(w=>(t._1,w)))
				  .toDF("date", "topic").as[DateWord]
				  .groupBy("date", "topic").count
          .select('*, rankByDate as 'rank).filter('rank <= 10)
          .foreach(t=>println(t))
          // print JSON

          


            