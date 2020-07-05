//imports
import org.apache.log4j._
import org.apache.spark.sql.DataFrame
import java.io.File


// remove warnings
Logger.getLogger("org").setLevel(Level.ERROR)


// Function "fcReadGpx" reads an input GPX and returns a Dataframe
def fcReadGpx(file: File): DataFrame = {
    spark.read.
    format("com.databricks.spark.xml").
    option("rootTag", "gpx.trk.trkseg").
    option("rowTag", "trkpt").
    load(file.getAbsolutePath).
    withColumn(
      "_ref",lit(file.getName)
    ).
    orderBy("time").
    withColumn(
      "tot_s",row_number().over(
        org.apache.spark.sql.expressions.Window.
        partitionBy(col("_ref")).
        orderBy(monotonically_increasing_id()
      )
    )-1)
}


// Function "fcIngestAll" recursively collects activity Dataframes
def fcIngestAll(files: List[File], seq: Seq[DataFrame]): Seq[DataFrame] = {
    if (0 == files.size)
      seq
    else
      fcIngestAll(files.init, seq :+ fcReadGpx(files.last))
}


// Table "trackingpoints" contains all GPX raw data
val dfTrackingpoints = fcIngestAll(
  // list GPX files from data directory
  new File("data").listFiles.filter(_.getName.endsWith(".gpx")).toList,
  Nil
).reduce(_ union _)
//dfTrackingpoints.show()
dfTrackingpoints.createOrReplaceTempView("trackingpoints")


// //debug
// println("DEBUG: dfTrackingpoints")
//dfTrackingpoints.printSchema()
//dfTrackingpoints.show()


// output tracking point count by activity
dfTrackingpoints.groupBy("_ref").count().show()


// Function "fcCalcDist" calculates distance in meters between 2 locations
def fcCalcDist(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  //  val avgEarthRadius = 6371008.3 // mean radius of the earth in meters
  val avgEarthRadius = 8926000 // custom radius for matching actual distance
  val deltaLat = Math.toRadians(lat2 - lat1)
  val deltaLng = Math.toRadians(lat2 - lat1)
  val a = Math.pow(Math.sin(deltaLat / 2), 2) + (
      Math.cos(Math.toRadians(lat2)) *
      Math.cos(Math.toRadians(lat1)) *
      Math.pow(Math.sin(deltaLng / 2), 2)
    )
  val greatCircleDist = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  BigDecimal(avgEarthRadius * greatCircleDist).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
}
val udfCalcDist = udf(fcCalcDist _)


// Table "distances" contains tracking segment distances
val dfDistances = spark.sql(
  " SELECT TP1._ref, " +
  "    TP2.tot_s as tot_s, " +
  "    TP1._lat as lat1, TP1._lon as lon1, " +
  "    TP2._lat as lat2, TP2._lon as lon2 " +
  " FROM trackingpoints TP1 INNER JOIN trackingpoints TP2 " +
  "    ON TP1.tot_s + 1 = TP2.tot_s " + // unix_timestamp(to_timestamp(TP1.time), 'MM-dd-yyyy HH:mm:ss')
  "    AND TP1._ref = TP2._ref "
).withColumn(
  "dist_m", udfCalcDist(
    col("lat1"), col("lon1"),
    col("lat2"), col("lon2")
  )
).select("_ref","tot_s","dist_m").orderBy("_ref","tot_s")
dfDistances.createOrReplaceTempView("distances")


// //debug
// println("DEBUG: dfDistances")
// dfDistances.show()
// dfDistances.groupBy("_ref").sum("dist_m").show()
// dfDistances.groupBy("_ref").max("tot_s").show()


// Mark "trackingpoints" for deletion
dfTrackingpoints.unpersist() // specify "blocking = true" to block until done


// Table "cumultot" contains cumulative total distance
val dfCumultot = spark.sql(
  " SELECT _ref, 0 AS tot_s, 0 AS dist_m, 0 AS tot_m " +
  " FROM distances " +
  " WHERE tot_s = 1 " + // insert "start" measure
  " UNION " +
  " SELECT _ref, tot_s, dist_m, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY _ref " +
  "        ORDER BY _ref,tot_s " +
  "        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
  "    ) tot_m " +
  " FROM distances "
).orderBy(
  "_ref","tot_s"
)
dfCumultot.createOrReplaceTempView("cumultot")


// //debug
// println("DEBUG: dfCumultot")
// dfCumultot.show()
dfCumultot.groupBy("_ref").max("tot_s","tot_m").show()
// dfCumultot.groupBy("_ref").sum("dist_m").show()


// Mark "distances" for deletion
dfDistances.unpersist() // specify "blocking = true" to block until done


// Table "cumulwin" contains cumulative distance by pace aggregation window
val pace_agg_window = 5 // we want to calculate avg pace on n data points
val dfCumulwin = spark.sql(
  " SELECT _ref, tot_s, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY _ref " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN " + (pace_agg_window - 1) + " PRECEDING AND CURRENT ROW" +
  "    ) dist_m, tot_m " +
  " FROM cumultot "
).filter(
  $"tot_s" % pace_agg_window === 0 // remove intermediary rows
)
dfCumulwin.createOrReplaceTempView("cumulwin")

// //debug
// println("DEBUG: dfCumulwin")
// dfCumulwin.show()
// dfCumulwin.groupBy("_ref").sum("dist_m").show()
// dfCumulwin.groupBy("_ref").max("tot_m","tot_s").show()


// write dataframe to file
dfCumulwin.write.parquet("data/dfCumulwin.parquet")


// Mark "cumultot" for deletion
dfCumultot.unpersist() // specify "blocking = true" to block until done


// Function "fcCalcPace" calculates running pace in min per km
def fcCalcPace(dMeter: Double, nSec: Int): String = {
  val secPerKm = nSec * 1000/(dMeter + 0.001) // +1mm to prevent division by zero
  ("" +
    (secPerKm / 60).toInt + ":" +
    (if (secPerKm % 60 < 10) "0" else "") + (secPerKm % 60).toInt
  )
}
val udfCalcPace = udf(fcCalcPace _)


// Table "fastpace" contains running pace and category "in" or "out"
val fast_split_m = (1000*pace_agg_window:Double) / 270    // catagorize splits above/below 4:30 or 270 sec/km
val dfClassification = spark.sql(
  " SELECT CW1._ref, " +
  "     CW2.tot_s AS tot_s, CW2.tot_m AS tot_m, " +
  "     CW2.tot_m - CW1.tot_m AS win_m, " +
  "     CASE WHEN (" +
  "       (CW2.tot_m - CW1.tot_m) > " + fast_split_m + ") " +
  "     THEN 'I' ELSE 'O' " +
  "     END AS cat " +
  " FROM cumulwin CW1 INNER JOIN cumulwin CW2 " +
  "    ON CW1._ref = CW2._ref " +
  "    AND CW1.tot_s + " + (pace_agg_window) + " = CW2.tot_s "
).withColumn(
  "pace_min_km", udfCalcPace(
    col("win_m"), lit(pace_agg_window)
  )
).orderBy("_ref", "tot_s")
dfClassification.createOrReplaceTempView("classification")

// //debug
// println("DEBUG: dfClassification")
dfClassification.show()
// dfClassification.groupBy("_ref").sum("win_m").show()
// dfClassification.groupBy("_ref").max("tot_m","tot_s").show()


// Mark "cumulwin" for deletion
dfCumulwin.unpersist() // specify "blocking = true" to block until done


// Training report by category (int/out)
val dfResult = spark.sql(
  " SELECT _ref, cat, " +
  "   ROUND(SUM(win_m), 2) AS dist_m, " +
  "   (COUNT(win_m) * " + pace_agg_window + ") AS time_s " +
  " FROM classification " +
  " GROUP BY _ref, cat "
).withColumn(
  "pace_min_km", udfCalcPace(
     col("dist_m"), col("time_s")
  )
).orderBy("_ref", "cat")

// //debug
// println("DEBUG: dfResult")
dfResult.show()
//dfResult.groupBy("_ref").sum("dist_m", "time_s").show()
