import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class Flights(flight_number: String, dep_icao: String, arr_icao: String, speed: Integer, status: String, lat: Double, lng: Double, alt: Integer)
/*
case class Flights(
                    hex: String, reg_number: String, flag: String, lat: Double, lng: Double,
                    alt: Integer, dir: Integer, speed: Integer, v_speed: Double, squawk: String,
                    flight_number: String, flight_icao: String, flight_iata: String, dep_icao: String,
                    dep_iata: String, arr_icao: String, arr_iata: String, airline_icao: String,
                    airline_iata: String, aircraft_icao: String, updated: Long, status: String
                  )
*/
object Main {
  def main(args: Array[String]): Unit = {

    // initialize Spark
    val spark = SparkSession
      .builder
      .appName("Main")
      .config("spark.master", "local[*]")
      .getOrCreate()



    import spark.implicits._

    // read from Kafka
    val inputDF = spark
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data")
      .load()

    val flightJsonDf = inputDF.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("flight_number", DataTypes.StringType)
      .add("dep_icao", DataTypes.StringType)
      .add("arr_icao", DataTypes.StringType)
      .add("speed", DataTypes.IntegerType)
      .add("status", DataTypes.StringType)
      .add("lat", DataTypes.DoubleType)
      .add("lng", DataTypes.DoubleType)
      .add("alt", DataTypes.IntegerType)

    val flightCoordinatesDf = flightJsonDf.select(from_json($"value", ArrayType(schema)).as("flights"))
      .selectExpr("inline(flights)")
      .as[Flights]



    val jdbcUsername = "dataman"
    val jdbcPassword = "aA0102"
    val jdbcHostname = "localhost"
    val jdbcPort = "3306"
    val jdbcDatabase = "realtime"

    val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val query = flightCoordinatesDf.writeStream
      .foreachBatch { (batchDF: Dataset[Flights], batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", "flights_table")
          .option("user", jdbcUsername)
          .option("password", jdbcPassword)
          .mode(SaveMode.Overwrite)
          .save()
      }
      .start()


    query.awaitTermination()
    /*
        val query1 = flightCoordinatesDf
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    //query.awaitTermination()
    query1.awaitTermination()
    */
  }
}