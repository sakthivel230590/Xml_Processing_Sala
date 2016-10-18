
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import com.databricks.spark.xml
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext;

object simple {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("example")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    
    
    loadxData(sqlcontext)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    def loadxData(sqlContext: SQLContext) = {
      var df5: DataFrame = null
      var newDf: DataFrame = null

      import  sqlContext.implicits._
      
      ## this loads the data from HDFS and parse the xml data
	  df5 = hiveContext.read.format("xml").option("rootTag","credit_summary").load("hdfs:///sakthi/new23.xml")
             df5. printSchema()
			 
}
}
}
