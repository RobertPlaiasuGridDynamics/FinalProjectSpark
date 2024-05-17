import com.sun.jmx.mbeanserver.Util.cast
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, min, trim}
import org.apache.spark.sql.types.{DecimalType, DoubleType}

import scala.annotation.tailrec

object Main extends App{

 Logger.getLogger("org").setLevel(Level.ERROR)

 val spark = SparkSession
   .builder
   .appName("FinalProjectSpark")
   .master("local[*]")
   .getOrCreate()


 import spark.implicits._
 val datasetWarehouse = spark
   .read
   .option("delimiter",", ")
   .option("header","true")
   .option("inferSchema","true")
   .csv("data/warehouse.csv")
   .as[Warehouse]

 val datasetRecords = spark
   .read
   .option("delimiter",", ")
   .option("header","true")
   .option("inferSchema","true")
   .csv("data/amounts.csv")
   .withColumn("amount", $"amount".cast(DecimalType(9,2)))
   .as[Records]

  val transformedRecords = datasetRecords
    .groupBy("positionId")
    .agg(max("eventTime"))



 val transformedWarehouse =datasetWarehouse.as("d1")
    .join(transformedRecords.as("d2"),
              datasetWarehouse("positionId") === transformedRecords("positionId"),
              "inner")
   .select("d1.positionId","warehouse","product","max(eventTime)")




 val finalWarehouse = transformedWarehouse.as("d4")
    .join(datasetRecords,transformedWarehouse("positionId") === datasetRecords("positionId")
    && transformedWarehouse("max(eventTime)") === datasetRecords("eventTime"),
    "inner")
    .select("d4.positionId","warehouse" ,"product","amount")
    .orderBy("d4.positionId")


  finalWarehouse.show()


  finalWarehouse
    .groupBy("warehouse","product")
    .agg(max("amount"),min("amount"),avg("amount"))
    .show()



 spark.stop()
}
