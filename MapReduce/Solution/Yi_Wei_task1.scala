import org.apache.spark.sql.SparkSession
object task1 {
    def main(args: Array[String]): Unit = {
        val path_json=args(0)
        val path_output=args(1)
        val spark = SparkSession
          .builder()
          .appName("task1")
          .master("local[*]")
          .getOrCreate()
        import spark.implicits._
        val df = spark.read.json(path_json)
        val rating_df=df.select("asin","overall")
        val result_df=rating_df.groupBy("asin").avg("overall").orderBy("asin")
        val newNames = Seq("asin", "rating_avg")
        val csv_df = result_df.toDF(newNames: _*)
        csv_df.coalesce(1).write.format("csv").option("header", "true").save(path_output)
  }
}



