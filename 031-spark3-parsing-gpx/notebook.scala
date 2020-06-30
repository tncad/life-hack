// Table "trackingpoints"
val dfTrackingpoints = spark.read.
    format("com.databricks.spark.xml").
    option("rootTag", "gpx.trk.trkseg").
    option("rowTag", "trkpt").
    load("data/activity_5098098091.gpx").
    orderBy("time").
    withColumn("tot_s",monotonically_increasing_id())
dfTrackingpoints.createOrReplaceTempView("trackingpoints")
//dfTrackingpoints.show()

//// output schema
//dfTrackingpoints.printSchema()
// output size
println("Count: " + dfTrackingpoints.count())


// Function "fcCalcDist" calculates distance in meters between 2 locations
def fcCalcDist(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
  val avgEarthRadius = 6371000 // meters
  val latDist = Math.toRadians(lat2 - lat1)
  val lngDist = Math.toRadians(lat2 - lat1)
  val sinLat = Math.sin(latDist / 2)
  val sinLng = Math.sin(lngDist / 2)
  val a = sinLat * sinLat + (
      Math.cos(Math.toRadians(lat2)) *
      Math.cos(Math.toRadians(lat1)) *
      sinLng * sinLng
    )
  val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  (avgEarthRadius * c)
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
    col("lat1"),col("lon1"),
    col("lat2"),col("lon2")
  )
).select("tot_s","dist_m")
//dfDistances.show()
dfDistances.createOrReplaceTempView("distances")


// Mark "trackingpoints" for deletion
dfTrackingpoints.unpersist() // specify "blocking = true" to block until done


// Table "cumuldist"
val dfCumuldist = spark.sql("" +
  " SELECT tot_s, dist_m, " +
  "    SUM(dist_m) OVER(" +
  "        PARTITION BY '1' " +
  "        ORDER BY tot_s " +
  "        ROWS BETWEEN unbounded preceding AND CURRENT ROW" +
  "    ) tot_m " +
  " FROM distances "
)
//dfCumuldist.show()
dfCumuldist.createOrReplaceTempView("cumuldist")


// Mark "distances" for deletion
dfDistances.unpersist() // specify "blocking = true" to block until done


// Function "fcCalcPace" calculates running pace in min per km
def fcCalcPace(dMeter: Double, nSec: Int): String = {
  val secPerKm = nSec * 1000/(dMeter + 0.01) // +1cm to prevent divide by zero
  ("" +
    (secPerKm / 60).toInt + ":" +
    (if (secPerKm % 60 < 10) "0" else "") + (secPerKm % 60).toInt
  )
}
val udfCalcPace = udf(fcCalcPace _)


// Dataframe "fastpace"
val agg_time_s = 5
val fast_split_m = 1000*agg_time_s/285  // mark splits faster than 4:45 min or 285 sec/km
val dfFastpace = spark.sql("" +
  " SELECT round(CD2.tot_s, 2) AS tot_s, " +
  "     round(CD2.tot_m, 2) AS tot_m, " +
  "     round(CD2.tot_m - CD1.tot_m, 2) AS win_m, " +
  "     CASE WHEN round(CD2.tot_m - CD1.tot_m, 2) > " + fast_split_m + " THEN 1 ELSE 0 END AS f " +
  " FROM cumuldist CD1 INNER JOIN cumuldist CD2 " +
  "    ON CD1.tot_s + " + agg_time_s + " = CD2.tot_s "
).withColumn(
  "pace_min_km", udfCalcPace(
    col("win_m"), lit(agg_time_s)
  )
).orderBy("tot_s")
//dfFastpace.show()

// Distance ran by pace category (in/out)
dfFastpace.groupBy("f").sum("win_m").show()
