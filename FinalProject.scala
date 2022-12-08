import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType} import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object finalProject{
def main(args: Array[String]) {

val sqlContext = new org.apache.spark.sql.SQLContext(sc); import sqlContext.implicits._

val schema = StructType( List(
StructField("age", IntegerType, true), StructField("job", StringType, true), StructField("marital", StringType, true), StructField("education", StringType, true), StructField("default", StringType, true), StructField("balance", IntegerType, true), StructField("housing", StringType, true), StructField("loan", StringType, true), StructField("contact", StringType, true), StructField("day", IntegerType, true), StructField("month", StringType, true), StructField("duration", IntegerType, true), StructField("campaign", IntegerType, true), StructField("pdays", IntegerType, true), StructField("previous", IntegerType, true), StructField("poutcome", StringType, true), StructField("y", StringType, true) ))

val df = spark.readStream.schema(schema).option("delimiter",";").csv("/user/maria_dev/FinalProject/
*")
df.printSchema() df.createOrReplaceTempView("bankdata") print("Give marketing success rate:")
 
val success =spark.sql("Select count(case y when 'yes' then 1 end)/count(*)*100 as success_rate from bankdata") success.writeStream.outputMode("update").option("truncate",false).format("console").start()
.awaitTermination(10)

print("Give marketing failuer rate:")
val fail=spark.sql("Select count(case y when 'no' then 1 end)/count(*)*100 as failure_rate from bankdata") fail.writeStream.outputMode("update").option("truncate",false).format("console").start().aw aitTermination(10)

print("Giving the max min and avg age :")
val age="""select max(age),min(age),avg(age) from bankdata""" val bage = spark.sql(age)
bage.writeStream.outputMode("update").option("truncate",false).format("console").start().a waitTermination(10)


print("quality of customers by checking average balance, median balance of customers :") val avg="""select percentile_approx(balance, 0.5),avg(balance) from bankdata"""
val customer= spark.sql(avg) customer.writeStream.outputMode("update").option("truncate",false).format("console").star t().awaitTermination(10)

print("age matters in marketing subscription for deposit :")
val q5 ="""select age,y as subscription_status from bankdata group by age,y """ val ageMatters = spark.sql(q5) ageMatters.writeStream.outputMode("update").option("truncate", false).format("console").start().awaitTermination(10)

print("marital status mattered for subscription to deposit:")
val q6 ="""select marital,y as subscription_status from bankdata group by marital,y""" val status = spark.sql(q6) status.writeStream.outputMode("update").option("truncate", false).format("console").start().awaitTermination(10)

print(" age and marital status together mattered for subscription to deposit scheme:")
val q7 ="""select age,marital,y as subscription_status from bankdata group by marital,y,age""" val together= spark.sql(q7) together.writeStream.outputMode("update").option("truncate",false).format("console").start ().awaitTermination(10)

}
}