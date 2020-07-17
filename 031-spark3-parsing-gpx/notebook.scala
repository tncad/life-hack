//imports
import org.apache.spark.sql.DataFrame
import java.io.File


// remove warnings
sc.setLogLevel("ERROR")


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
        partitionBy($"_ref").
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
  new File("data/1_transient").listFiles.filter(_.getName.endsWith(".gpx")).toList,
  Nil
).reduce(_ union _)
dfTrackingpoints.createOrReplaceTempView("trackingpoints")


// persist to raw zone
var out = new File("data/2_raw/gpx.parquet")
if (out.exists && out.isDirectory) {
  for (f <- out.listFiles.toList ) { f.delete }
  out.delete
}
dfTrackingpoints.write.parquet("data/2_raw/gpx.parquet")


// output tracking point count by activity
dfTrackingpoints.groupBy("_ref").count().show()


// Function "fcCalcDist" calculates distance in meters between 2 locations
def fcCalcDist(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  //val avgEarthRadius = 6371008.3 // mean radius of the earth in meters
  val avgEarthRadius = if (lon1 < 7.13) 7042000 else 8946000 // custom radius for matching actual distance depending on training location
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
  "    TP2._lat as lat2, TP2._lon as lon2, " +
  "    TP2.ele as ele, " +
  "    TP2.extensions as ext " +  
  " FROM trackingpoints TP1 INNER JOIN trackingpoints TP2 " +
  "    ON TP1.tot_s + 1 = TP2.tot_s " + // unix_timestamp(to_timestamp(TP1.time), 'MM-dd-yyyy HH:mm:ss')
  "    AND TP1._ref = TP2._ref "
).withColumn(
  "dist_m", udfCalcDist(
    $"lat1", $"lon1",
    $"lat2", $"lon2"
  )
).select("_ref","tot_s","dist_m","ele","ext").orderBy("_ref","tot_s")
dfDistances.createOrReplaceTempView("distances")


// INFO
dfDistances.groupBy("_ref").sum("dist_m").show()


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
).withColumn(
  "pace_m_s",
  $"tot_m"/$"tot_s"
).orderBy(
  "_ref","tot_s"
)
dfCumultot.createOrReplaceTempView("cumultot")


// Mark "distances" for deletion
dfDistances.unpersist() // specify "blocking = true" to block until done


// Table "preagg" contains activity ref, time, distance, and mean pace
val dfPreAgg = dfCumultot.groupBy(
  "_ref"
).max(
  "tot_s","tot_m"
).withColumn(
  "mean_m_s",
  lit(0.1) + $"max(tot_m)"/$"max(tot_s)"
)
dfPreAgg.createOrReplaceTempView("preagg")


// INFO
dfPreAgg.show()


// Table "cumulwin" contains cumulative distance by pace aggregation window
val pace_agg_window = 3 // we want to calculate avg pace on n data points
val dfCumulwin = spark.sql(
  " SELECT _ref, tot_s, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY _ref " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN " + (pace_agg_window - 1) + " PRECEDING AND CURRENT ROW" +
  "    ) dist_m, " + 
  "    tot_m " +
  " FROM cumultot "
).filter(
  $"tot_s" % pace_agg_window === 0 // remove intermediary rows
)
dfCumulwin.createOrReplaceTempView("cumulwin")


// Mark "cumultot" for deletion
dfCumultot.unpersist() // specify "blocking = true" to block until done


// Table "classification" contains running pace and category
//val fast_split_m = (mean_m_s * pace_agg_window):Double
val dfClassification = spark.sql(
  " SELECT CW1._ref, " +
  "     CW2.tot_s, CW2.tot_m, " +
  "     CW2.tot_m - CW1.tot_m AS win_m, " +
  "     CASE WHEN (" +
  "       (CW2.tot_m - CW1.tot_m) > (PA.mean_m_s * " + pace_agg_window + ")" +
  "     ) THEN '1' ELSE '0' " +
  "     END AS cat " +
  " FROM cumulwin CW1 " +
  "    INNER JOIN cumulwin CW2 " +
  "      ON CW1._ref = CW2._ref " +
  "      AND CW1.tot_s + " + (pace_agg_window) + " = CW2.tot_s " +
  "    LEFT JOIN preagg PA " +
  "      ON CW1._ref = PA._ref "
).orderBy("_ref", "tot_s")
dfClassification.createOrReplaceTempView("classification")


// INFO
dfClassification.show()


// Mark "cumulwin" and "preagg" for deletion
dfCumulwin.unpersist() // specify "blocking = true" to block until done
dfPreAgg.unpersist()


// Function "fcCalcPace" calculates running pace in min per km
def fcSetCat2(weight: Int): Int = {
  if (weight > 3) 1 else 0
}
val udfSetCat2 = udf(fcSetCat2 _)


// Table "classification2" contains 2nd category pass
val dfClassification2 = spark.sql(
  " SELECT *, " +
  "    SUM(cat) OVER(" +
  "        PARTITION BY _ref " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" +
  "    ) weight " +
  " FROM classification "
).withColumn(
  "cat2", udfSetCat2($"weight")
).orderBy("_ref", "tot_s")
dfClassification2.createOrReplaceTempView("classification2")


// INFO
dfClassification2.show()


// persist to trusted zone
out = new File("data/3_trusted/gpx.parquet")
if (out.exists && out.isDirectory) {
  for (f <- out.listFiles.toList ) { f.delete }
  out.delete
}
dfCumulwin.write.parquet("data/3_trusted/gpx.parquet")


// Function "fcCalcPace" calculates running pace in min per km
def fcCalcPace(dMeter: Double, nSec: Int): String = {
  val secPerKm = nSec * 1000/(dMeter + 0.001) // +1mm to prevent division by zero
  ("" +
    (secPerKm / 60).toInt + ":" +
    (if (secPerKm % 60 < 10) "0" else "") + (secPerKm % 60).toInt
  )
}
val udfCalcPace = udf(fcCalcPace _)


// Training report by category (int/out)
val dfResult = spark.sql(
  " SELECT _ref, cat2, " +
  "   ROUND(SUM(win_m), 2) AS dist_m, " +
  "   (COUNT(win_m) * " + pace_agg_window + ") AS time_s " +
  " FROM classification2 " +
  " GROUP BY _ref, cat2 "
).withColumn(
  "pace_min_km", udfCalcPace(
     $"dist_m", $"time_s"
  )
).orderBy("_ref", "cat2")


// INFO
dfResult.show()


// persist to refined zone
out = new File("data/4_refined/gpx.csv")
if (out.exists) {
  out.delete
}
dfResult.write.csv("data/4_refined/gpx.csv")
