import org.apache.spark.sql.SparkSession
object task2 {
  def main(args: Array[String]): Unit = {
    val path_rating=args(0)
    val path_brand=args(1)
    val path_output=args(2)
    val spark = SparkSession
      .builder()
      .appName("task1")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df_r = spark.read.json(path_rating)
    val df_m = spark.read.json(path_brand)
    val df_rating=df_r.select("asin","overall")
    val df_brand=df_m.select("asin","brand")
    val df_join = df_rating.join(df_brand,"asin").filter($"brand".isNotNull).filter("brand != ''").select("brand","overall")
    val df_result=df_join.groupBy("brand").avg("overall").orderBy("brand")
    val newNames = Seq("brand", "rating_avg")
    val csv_df = df_result.toDF(newNames: _*)
    csv_df.coalesce(1).write.format("csv").option("header", "true").save(path_output)
  }
}
