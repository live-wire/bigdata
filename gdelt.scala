package gdelt

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
import org.apache.spark.sql.functions.collect_list

object GDelt {
  case class GDeltClass (
      DATE: Timestamp,
      AllNames: String
  )
  case class DateWord(date: String, topic: String)

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
  def main(args: Array[String]) {
    val files = if (args.size > 0) args(0) else "10"
    println("Files to process = " + files)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    // val segments = Option(new File("./segment/").list).map(_.filter(_.endsWith(".csv")).size).getOrElse(0);
    val spark = SparkSession
      .builder
      .appName("GDelt")
      // .config("spark.master", "local") // Commenting this to remove the error-code-13 in EMR
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._
    // val nsegments = Option(new File("./segment/").list).map(_.filter(_.endsWith(".csv")).size).getOrElse(0)
    // println("\n\n Running on " + nsegments + " segments ======>")

    // var csvrow = "\n" + nsegments + ", "
    var timediff = 0.0

    // var csvrow = "\n"
    // println("\n\n RDD implementation below: \n\n")
    // val trdd2 = System.nanoTime()
    // rddImplementation(sc, files)
    // timediff = (System.nanoTime() - trdd2).toDouble / pow(10, 9).toDouble
    // // csvrow += timediff + ", "
    // println("Elapsed time: " + timediff + " seconds")


    // println("\n\n Dataset-V2 implementation below: \n\n")
    // val tds2 = System.nanoTime()
    // dsImplementationV2(sc, spark)
    // timediff = (System.nanoTime() - tds2).toDouble / pow(10, 9).toDouble
    // csvrow += timediff + ", "
    // println("Elapsed time: " + timediff + " seconds")
    // val fw = new FileWriter("runtime.csv", true);
    // fw.write(csvrow);
    // fw.close()


    println("\n\n New Dataset-V3 implementation below: \n\n")
    val tds3 = System.nanoTime()
    dsImplementationV3(sc, spark, files)
    timediff = (System.nanoTime() - tds3).toDouble / pow(10, 9).toDouble
    // csvrow += timediff + ", "
    println("Elapsed time: " + timediff + " seconds")
    spark.stop
  }

  def prepareRangeString(num: String) : String = {
    if (num == "*") {
      return "*"
    } else if ( num == "10" ) {
      return "2015021{8,900,901[01]}*"
    } else if ( num == "100" ) {
      return "201502{18,19,200[01]}*"
    } else if ( num == "1000") {
      return "20150{2,3010,30110}*"
    } else if ( num == "10000") {
      return "20150{[12345],60[12],6030[0123456],603070000}*"
    } else if ( num == "100000") {
      return "201{[567],8010[01234567],801080,801081[01234567],80108180000}*"
    }
    return "*"
  }

  // def rddImplementation(sc: org.apache.spark.SparkContext, files: String) {

  //   val gdeltv2 = sc.textFile("s3n://gdelt-open-data/v2/gkg/" + prepareRangeString(files) + ".csv") // Array[String] Reads all csv files inside the segment folder
  //                 .map(s=>s.split("\t")) // Array[Array[String]]
  //                 .filter(a=>a.size>23 && a(23)!="") // Array[Array[String]]
  //                 .map(a=>(a(1).substring(0, 4)+"-" + a(1).substring(4, 6) + "-" + a(1).substring(6, 8), 
  //                         a(23).split(";")))
  //                 .map(t=>(t._1,t._2.map(tin=>tin.split(",")(0)).distinct))
  //                 .flatMap(t=>t._2.map(w=>((t._1,w),1)))
  //                 .reduceByKey((x,y)=>x+y)
  //                 .groupBy(t=>t._1._1)
  //                 .mapValues(t=>t.map(tin=>(tin._1._2,tin._2))
  //                                .toArray.sortBy(t=>t._2)
  //                                .reverse.take(10))
  //                 .collect()
  //                 // print RDD 
  //                 // .foreach(t=>println(t._1,t._2.mkString(" ")))
                  
  //                 // print JSON
  //                 .map(t=>compact(render(
  //                         ("data"->t._1)~
  //                         ("result"->t._2.toList.map{tin=>(
  //                                     ("topic"->tin._1)~
  //                                     ("count"->tin._2))}
  //                         ))))
  //                 .map(s=>println(s))
  // }

  def dsImplementationV3(sc: org.apache.spark.SparkContext, spark: SparkSession, files: String) {
    import spark.implicits._
    val ds = spark.read
                  .schema(schema)
                  .option("timestampFormat", "yyyyMMddhhmmss")
                  .option("delimiter", "\t")
                  .csv("s3n://gdelt-open-data/v2/gkg/" + prepareRangeString(files) + ".csv")
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
            .drop("rank")
            .rdd
            .map(r=>(r.getString(0),List((r.getString(1), r.getLong(2)))))
            .reduceByKey((x,y)=>x++y)
            .collect()
            // print JSON
            .map(t=>compact(render(
                    ("data"->t._1)~
                    ("result"->t._2.toList.map{tin=>(
                                ("topic"->tin._1)~
                                ("count"->tin._2))}
                    ))))
            .map(s=>println(s))
            
  }
}