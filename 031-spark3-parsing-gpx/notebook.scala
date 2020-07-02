// Function "fcReadGpx" reads an input GPX and returns a Dataframe
def fcReadGpx(file: java.io.File): org.apache.spark.sql.DataFrame = {
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
def fcIngestAll(files: List[java.io.File], seq: Seq[org.apache.spark.sql.DataFrame]): Seq[org.apache.spark.sql.DataFrame] = {
    if (0 == files.size)
      seq
    else
      fcIngestAll(files.init, seq :+ fcReadGpx(files.last))
}


// Table "trackingpoints" contains all GPX raw data
val dfTrackingpoints = fcIngestAll(
  // list GPX files from data directory
  new java.io.File("data").listFiles.filter(_.getName.endsWith(".gpx")).toList,
  Nil
).reduce(_ union _)

// output tracking point count by activity
dfTrackingpoints.groupBy("_ref").count().show()
//System.exit(0)

//// output schema
//dfTrackingpoints.printSchema()
dfTrackingpoints.createOrReplaceTempView("trackingpoints")


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
//dfDistances.show()
dfDistances.createOrReplaceTempView("distances")


// Mark "trackingpoints" for deletion
dfTrackingpoints.unpersist() // specify "blocking = true" to block until done


// Table "cumultot" contains cumulative total distance
val dfCumultot = spark.sql(
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
//dfCumultot.show()
dfCumultot.groupBy("_ref").agg(max(col("tot_m")).alias("max_m")).show()
dfCumultot.createOrReplaceTempView("cumultot")


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
//dfCumulwin.show()
dfCumulwin.createOrReplaceTempView("cumulwin")


// Mark "cumultot" for deletion
dfCumultot.unpersist() // specify "blocking = true" to block until done


// Function "fcCalcPace" calculates running pace in min per km
def fcCalcPace(dMeter: Double, nSec: Int): String = {
  val secPerKm = nSec * 1000/(dMeter + 0.01) // +1cm to prevent divide by zero
  ("" +
    (secPerKm / 60).toInt + ":" +
    (if (secPerKm % 60 < 10) "0" else "") + (secPerKm % 60).toInt
  )
}
val udfCalcPace = udf(fcCalcPace _)


// Table "fastpace" contains running pace and category "in" or "out"
val fast_split_m = 1000*pace_agg_window/251  // catagorize splits above/below 4:11 or 251 sec/km
val dfClassification = spark.sql(
  " SELECT CW1._ref, " +
  "     CW2.tot_s AS tot_s, " +
  "     CW2.tot_m AS tot_m, " +
  "     CW2.tot_m - CW1.tot_m AS win_m, " +
  "     CASE WHEN ((CW2.tot_m - CW1.tot_m) > " + fast_split_m + ") THEN 'I' ELSE 'O' END AS cat " +
  " FROM cumulwin CW1 INNER JOIN cumulwin CW2 " +
  "    ON CW1.tot_s + " + pace_agg_window + " = CW2.tot_s " +
  "    AND CW1._ref = CW2._ref "
).withColumn(
  "pace_min_km", udfCalcPace(
    col("win_m"), lit(pace_agg_window)
  )
).orderBy("_ref", "tot_s")
dfClassification.show()
dfClassification.createOrReplaceTempView("classification")


// Mark "cumulwin" for deletion
dfCumulwin.unpersist() // specify "blocking = true" to block until done


// Training report by category (int/out)
val dfResult = spark.sql(
  " SELECT _ref, cat, " +
  "   ROUND(SUM(win_m), 2) AS dist_m, " +
  "   COUNT(win_m) * " + pace_agg_window + " AS time_s " +
  " FROM classification " +
  " GROUP BY _ref, cat"
)
dfResult.withColumn(
  "pace_min_km", udfCalcPace(
     col("dist_m"), col("time_s")
  )
).orderBy("_ref","cat").show()
