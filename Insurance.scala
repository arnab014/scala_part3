import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
​
object Insurance {
  def main(args : Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Insurance").setMaster("local[*]")
    val sc = new SparkContext(conf)
​
    val txtRDD = sc.textFile("insurance.csv")
​
    import org.apache.spark.util.SizeEstimator
    println(SizeEstimator.estimate(txtRDD))
​
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
​
    val df = spark.read.format("csv").option("header", "true").load("insurance.csv")

    // Print sex and count of sex 
    df.groupBy("sex").count().show()
​
    // Print sex and count of sex, Filter smoker=yes
    df.groupBy("sex").count().filter("smoker=='yes'").show()
​
    //Group by region and sum the charges and print rows by desc order
    df.groupBy("region").agg(sum("charges")as("Total")).sort(desc("Total")).show()
  }
}