package Part3

import java.io._

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{Row, SparkSession}

object TFIDF {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TfIdfSpark")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "2g")

    val sc = SparkSession.builder.config(conf).getOrCreate()
    import sc.implicits._

    val query = sc.read.format("csv").option("header", "true").load(args(0))
    val trainQuery = sc.read.format("csv").option("header", "true").load(args(1))

    val newestEventPerUser = trainQuery.groupBy("order_id").agg(collect_list("product_id").as("product_id"))
    val fpgrowth = new FPGrowth().setItemsCol("product_id").setMinSupport(0.005).setMinConfidence(0.2)
    val model = fpgrowth.fit(newestEventPerUser)

    val inter = query.select("product_id","product_name").rdd.map { case Row(id: String, string: String) => (id, string) }
    val vocab = inter.collectAsMap()
    val termsIdx2Str = udf { (termIndices: Seq[String]) => termIndices.map(idx => vocab.get(idx)) }

    val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")

    val freqItemsets = model.freqItemsets.withColumn("terms", stringify(termsIdx2Str(col("items")))).orderBy($"freq".desc).limit(10).select("terms").collect().mkString("\n")
    val asscRules = model.associationRules.withColumn("antecedent", stringify(termsIdx2Str(col("antecedent")))).withColumn("consequent", stringify(termsIdx2Str(col("consequent")))).orderBy($"confidence".desc).limit(10).select("antecedent","consequent").collect().mkString("\n")
    val fs = FileSystem.get(sc.sparkContext.hadoopConfiguration)
    val path: Path = new Path(args(2))
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    bw.write("\nFrequent Item Sets\n")
    bw.write(freqItemsets)
    bw.write("\nAssociation Rules\n")
    bw.write(asscRules)
    bw.close
  }
}