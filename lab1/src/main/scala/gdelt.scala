package gdelt

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import java.io._

object GDelt {
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
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val segments = Option(new File("./segment/").list).map(_.filter(_.endsWith(".csv")).size).getOrElse(0);
    val spark = SparkSession
      .builder
      .appName("GDelt")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._
    
    println("Running on " + Option(new File("./segment/").list).map(_.filter(_.endsWith(".csv")).size).getOrElse(0) + " segments ======>")

    println("\n\n RDD implementation below: \n\n")
    val trdd = System.nanoTime()
    rddImplementation(sc)
    println("Elapsed time: " + (System.nanoTime() - trdd) + "ns")

    println("\n\n RDD-V2 implementation below: \n\n")
    val trdd2 = System.nanoTime()
    rddImplementationV2(sc)
    println("Elapsed time: " + (System.nanoTime() - trdd2) + "ns")

    println("\n\n Dataset implementation below: \n\n")
    val tds = System.nanoTime()
    dsImplementation(sc, spark)
    println("Elapsed time: " + (System.nanoTime() - tds) + "ns")

    println("\n\n Dataset-V2 implementation below: \n\n")
    val tds2 = System.nanoTime()
    dsImplementationV2(sc, spark)
    println("Elapsed time: " + (System.nanoTime() - tds2) + "ns")

    spark.stop
  }
  def rowReducer (key: String , arr: Array[(String, Int)], sc: org.apache.spark.SparkContext) : (String, Array[(String, Int)]) = {
        val gdeltDate = sc.parallelize(arr)
                    .reduceByKey((x,y)=>x+y)
                    .filter(t=>t._1!="")
                    .sortBy(_._2, false)
                    .take(10)
        println((key, gdeltDate.mkString(", ")))
        return (key, arr)
  }

  def rddImplementation(sc: org.apache.spark.SparkContext) {
    val gdelt = sc.textFile("./segment/*.csv") // Array[String] Reads all csv files inside the segment folder
                  .map(s=>s.split("\t")) // Array[Array[String]]
                  .filter(a=>a.size>23 && a(23)!="") // Array[Array[String]]
                  .map(a=>(a(1).substring(0, 4)+"-" + a(1).substring(4, 6) + "-" + a(1).substring(6, 8), 
                          a(23).split(";") // Array[Array[(String, Array[String])]]
                               .map(x=>x.split(",")(0)) // Array[Array[(String, Array[String])]]
                               .distinct  // Array[Array[(String, Array[String])]]
                               .map(x=>(x, 1))))  // Array[Array[(String, Array[(String, Int)])]]
                  // .filter(t=>t._1=="2015-02-18") // Uncomment this to enable the operation only for a particular date
                  .reduceByKey((x,y)=>x++y)
                  .collect() // Important before we can call multiple sc.parallelize
                  .map(t=>rowReducer(t._1, t._2, sc))
  }

  def rddImplementationV2(sc: org.apache.spark.SparkContext) {
    val gdeltv2 = sc.textFile("./segment/*.csv") // Array[String] Reads all csv files inside the segment folder
                  .map(s=>s.split("\t")) // Array[Array[String]]
                  .filter(a=>a.size>23 && a(23)!="") // Array[Array[String]]
                  .map(a=>(a(1).substring(0, 4)+"-" + a(1).substring(4, 6) + "-" + a(1).substring(6, 8), 
                          a(23).split(";")))
                  .map(t=>(t._1,t._2.map(tin=>tin.split(",")(0)).distinct))
                  .flatMap(t=>t._2.map(w=>((t._1,w),1)))
                  .reduceByKey((x,y)=>x+y)
                  .groupBy(t=>t._1._1)
                  .mapValues(t=>t.map(tin=>(tin._1._2,tin._2))
                                 .toArray.sortBy(t=>t._2)
                                 .reverse.take(10))
                  .collect()
                  .foreach(t=>println(t._1,t._2.mkString(" ")))
  }

  def dsImplementation(sc: org.apache.spark.SparkContext, spark: SparkSession) {
    import spark.implicits._
    val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "yyyyMMddhhmmss")
                  .option("delimiter", "\t")
                  .csv("./segment/*.csv")
                  .as[GDeltClass]

    val gdelt = ds.filter(line=>line.AllNames!=null && line.DATE!=null)
                  .map(t=>(t.DATE.toString().split(" ")(0), t.AllNames.split(";")
                                    .map(tup=>(tup.split(",")(0),1))
                                    .distinct))
                  .groupByKey(_._1)
                  .reduceGroups((x,y)=>(x._1, x._2 ++ y._2))
                  .map(_._2)
                  .collect()
                  .map(t=>rowReducer(t._1, t._2, sc))

  }

  def dsImplementationV2(sc: org.apache.spark.SparkContext, spark: SparkSession) {
    import spark.implicits._
    val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "yyyyMMddhhmmss")
                  .option("delimiter", "\t")
                  .csv("./segment/*.csv")
                  .as[GDeltClass]

    val gdelt = ds.filter(line=>line.AllNames!=null && line.DATE!=null)
                  .map(t=>(t.DATE.toString().split(" ")(0), t.AllNames.split(";")))
                  .map(t=>(t._1,t._2.map(tin=>tin.split(",")(0)).distinct))
                  .flatMap(t=>t._2.map(w=>((t._1,w),1)))
                  .rdd
                  .reduceByKey((x,y)=>x+y)
                  .groupBy(t=>t._1._1)
                  .mapValues(t=>t.map(tin=>(tin._1._2,tin._2))
                                 .toArray.sortBy(t=>t._2)
                                 .reverse.take(10))
                  .collect()
                  .foreach(t=>println(t._1,t._2.mkString(" ")))

  }
}