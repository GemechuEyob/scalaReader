import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.functions._


object ScalaReader {
  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val url  =  "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB"
    val username =  "jzh46612"
    val password =  "p253hd83m2jlx@z0"

    val spark = SparkSession.builder.master("local[1]").appName("ScalaReader").getOrCreate
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list","cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.impl","com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl","com.ibm.stocator.fs.cos.COSAPIClient")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme","cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.myCos.access.key","0aba66146f3b450cacebaa908046d17e")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.myCos.endpoint","https://s3.us.cloud-object-storage.appdomain.cloud")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.myCos.secret.key","27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")

    // read employee data
    val sqlContext = new SQLContext(spark.sparkContext)
    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("cos://candidate-exercise.myCos/emp-data.csv")

    // 1: display 15 records from input data
    df.show(15)

    // write to db2 database in JZH46612.employee
    df.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "JZH46612.employee")
      .option("user", username)
      .option("password", password)
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save()


    val employeeDF = sqlContext.load("jdbc",
      Map(
        "url" -> url,
        "user" -> username,
        "password" -> password,
        "driver" -> "com.ibm.db2.jcc.DB2Driver",
        "dbtable" -> "JZH46612.employee"
      )
    )

    // find gender ratio and display
    val genderRatioDF = employeeDF.groupBy("department")
      .agg(
        sum(expr("case when Gender='Male' then 1 else 0 end")).divide(count("*")).as("Male_ratio")
        ,sum(expr("case when Gender='Female' then 1 else 0 end")).divide(count("*")).as("Female_ratio")
      )
    genderRatioDF.show()

    // clean up salary values and get the average salary per dept
    val avgSalaryDF = employeeDF
      .withColumn("clean_salary",regexp_replace(col("salary"), "\\$" , ""))
      .withColumn("clean_salary",regexp_replace(col("clean_salary"), "," , ""))
      .withColumn("clean_salary",expr("cast(clean_salary as int)"))
      .groupBy("department")
      .agg(
        mean("clean_salary").as("average_salary")
      )
    avgSalaryDF.show()

    // get male and female avg salary and calculate the gap per dept.
    val salaryGapDF = employeeDF
      .withColumn("clean_salary",regexp_replace(col("salary"), "\\$" , ""))
      .withColumn("clean_salary",regexp_replace(col("clean_salary"), "," , ""))
      .withColumn("clean_salary",expr("cast(clean_salary as int)"))
      .groupBy("department")
      .agg(
        mean(expr("case when Gender='Male' then clean_salary else 0 end")).as("male_avg_salary")
        ,mean(expr("case when Gender='Female' then clean_salary else 0 end")).as("female_avg_salary")
      )
      .withColumn("salary_gap", abs(col("male_avg_salary") - col("female_avg_salary")))

    salaryGapDF.show()

    // write one of the reports to parquet
    salaryGapDF.write.mode("overwrite").parquet("cos://candidate-exercise.myCos/emp-salary-gap")

  }
}

//{
//  "db": "BLUDB",
//  "dsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=jzh46612;PWD=p253hd83m2jlx@z0;",
//  "host": "dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net",
//  "hostname": "dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net",
//  "https_url": "https://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net",
//  "jdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB",
//  "parameters": {
//    "role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Manager"
//  },
//  "password": "p253hd83m2jlx@z0",
//  "port": 50000,
//  "ssldsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net;PORT=50001;PROTOCOL=TCPIP;UID=jzh46612;PWD=p253hd83m2jlx@z0;Security=SSL;",
//  "ssljdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;",
//  "uri": "db2://jzh46612:p253hd83m2jlx%40z0@dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB",
//  "username": "jzh46612"
//}


//
//--
//-- The CREATE TABLE statement defines a table. In MPP, you should define a proper distribution key
//-- The distribution key helps performance by spreading table data evenly across data partitions and
//-- (with Hash) collocating grows commonly fetched together. Generally, specify few columns,
//-- and ones that have distinct, evenly distributed values.
//  --
//-- Modify the example statement below. Edit the {} variables. Choose an option in [].
//--
//CREATE TABLE "JZH46612".employee ( name CHAR(50) ,gender CHAR(50) ,department CHAR(50) ,salary CHAR(50) ,loc CHAR(50) ,rating CHAR(50) )           ORGANIZE BY COLUMN
//  -- DISTRIBUTE BY HASH( { column-name }, ..., { column-name } )
//-- PARTITION BY [RANGE] ( { column-name } [NULLS FIRST | NULLS LAST] )
//--          ( PARTITION { partition-name } STARTING ( { starting-value } ) ENDING ( { ending-value } ), ..., PARTITION { partition-name } STARTING ( { starting-value } ) ENDING ( { ending-value } ) )
//;
//--
//-- Optional: The ALTER TABLE statement defines a primary key for the table
//  -- Modify the example statement below
//  --
//-- ALTER TABLE { table-name }
//--           ADD CONSTRAINT { constraint-name }
//--           PRIMARY KEY ( { column-name, ..., column-name } )
//--		      ;
//--
//-- Optional: Grant table privileges to others
//-- GRANT { CONTROL, INDEX, SELECT, UPDATE, INSERT, ALTER, DELETE, REFERENCES }
//--          ON TABLE { table-schema }.{table-name} TO [public | user | role | group] ;