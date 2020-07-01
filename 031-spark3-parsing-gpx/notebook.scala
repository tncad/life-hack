//val activity_id = "5098098091"
//val activity_id = "5128178807"
val activity_id = "5166809451"

val dfTrackingpoints = spark.
    read.
    format("com.databricks.spark.xml").
    option("rootTag", "gpx.trk.trkseg").
    option("rowTag", "trkpt").
    load("data/activity_" + activity_id + ".gpx").
    withColumn(
      "_ref",lit(activity_id)
    ).
    orderBy("time").
    withColumn(
      "tot_s",row_number().over(
        org.apache.spark.sql.expressions.Window.
        partitionBy(col("_ref")).
        orderBy(monotonically_increasing_id()
      )
    )-1)
println("Count: " + dfTrackingpoints.count())
//dfTrackingpoints.show()
dfTrackingpoints.createOrReplaceTempView("trackingpoints")

//// output schema
//dfTrackingpoints.printSchema()


// Function "fcCalcDist" calculates distance in meters between 2 locations
def fcCalcDist(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  //  val avgEarthRadius = 6371008.3 // mean radius of the earth in meters
  val avgEarthRadius = 8995000 // custom radius for matching actual distance
  val deltaLat = Math.toRadians(lat2 - lat1)
  val deltaLng = Math.toRadians(lat2 - lat1)
  val a = Math.pow(Math.sin(deltaLat / 2), 2) + (
      Math.cos(Math.toRadians(lat2)) *
      Math.cos(Math.toRadians(lat1)) *
      Math.pow(Math.sin(deltaLng / 2), 2)
    )
  val greatCircleDist = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  BigDecimal(avgEarthRadius * greatCircleDist).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
}
val udfCalcDist = udf(fcCalcDist _)


// Table "distances"
val dfDistances = spark.sql("" +
  " select TP2.tot_s as tot_s, " +
  "    TP1._lat as lat1, TP1._lon as lon1, " +
  "    TP2._lat as lat2, TP2._lon as lon2 " +
  " from trackingpoints TP1 inner join trackingpoints TP2 " +
  "    on TP1.tot_s + 1 = TP2.tot_s " // unix_timestamp(to_timestamp(TP1.time), 'MM-dd-yyyy HH:mm:ss')
).withColumn(
  "dist_m", udfCalcDist(
    col("lat1"), col("lon1"),
    col("lat2"), col("lon2")
  )
).select("tot_s","dist_m").orderBy("tot_s")
//dfDistances.show()
dfDistances.createOrReplaceTempView("distances")


// Mark "trackingpoints" for deletion
dfTrackingpoints.unpersist() // specify "blocking = true" to block until done


// Table "cumultot" calculates cumulative total distance
val dfCumultot = spark.sql("" +
  " SELECT tot_s, dist_m, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY '1' " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" +
  "    ) tot_m " +
  " FROM distances "
).orderBy(
  "tot_s"
)
//dfCumultot.show()
dfCumultot.agg(max(col("tot_m")).alias("max_m")).show()
dfCumultot.createOrReplaceTempView("cumultot")


// Mark "distances" for deletion
dfDistances.unpersist() // specify "blocking = true" to block until done


// Table "cumulwin" calculates cumulative window distance
val pace_agg_window = 4 // we want to calculate avg pace on n data points
val dfCumulwin = spark.sql("" +
  " SELECT tot_s, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY '1' " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN " + (pace_agg_window - 1) + " PRECEDING AND CURRENT ROW" +
  "    ) dist_m, tot_m " +
  " FROM cumultot "
).filter(
  $"tot_s" % pace_agg_window === 0
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


// Dataframe "fastpace" calculates and categorize running pace
val fast_split_m = 1000*pace_agg_window/263  // mark splits faster than 4:23 min or 263 sec/km
val dfFastpace = spark.sql("" +
  " SELECT CW2.tot_s AS tot_s, " +
  "     CW2.tot_m AS tot_m, " +
  "     CW2.tot_m - CW1.tot_m AS win_m, " +
  "     CASE WHEN (CW2.tot_m - CW1.tot_m > " + fast_split_m + ") THEN 'I' ELSE 'O' END AS cat " +
  " FROM cumulwin CW1 INNER JOIN cumulwin CW2 " +
  "    ON CW1.tot_s + " + pace_agg_window + " = CW2.tot_s "
).withColumn(
  "pace_min_km", udfCalcPace(
    col("win_m"), lit(pace_agg_window)
  )
).orderBy("tot_s")
dfFastpace.show()


// Mark "cumulwin" for deletion
dfCumulwin.unpersist() // specify "blocking = true" to block until done


// Distance ran by pace category (in/out)
dfFastpace.groupBy("cat").sum("win_m").show()
