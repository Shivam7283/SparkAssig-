import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{avg, col, collect_list, max, min}
import shapeless.ops.nat.Max

import javax.swing.plaf.synth.Region
object SparkDemo {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("SParkDemoAssi-1")
      .master("local")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","file:////usr/local/spark-3.2.1-bin-hadoop3.2/spark-events")
      .config("spark.history.fs.logDirectory","file:////usr/local/spark-3.2.1-bin-hadoop3.2/spark-events")
      .getOrCreate()


    val df= spark.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/employee_address_details_new.csv")
//
//    // show record
//    df.show(2000,true)
//  df.show()
//    //df.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details.csv")
//
//
//    val df1=df.filter(df("Region")==="South")
//    val df2=df.filter(df("Region")==="Northeast")
//    val df3=df.filter(df("Region")==="Midwest")
//    val df4=df.filter(df("Region")==="West")
//    val df5=df.filter(df("Region")==="East")
//
//    // showing data
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
//
//    // storing data
////    df1.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details_south.csv")
////    df2.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details_Northeast.csv")
//    //df3.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details_midwest.csv")
//    //df4.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details_West.csv")
//
//   df.write.partitionBy("Region").csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/new_employee_address_details_partition.csv")

    // Data Loaded in hdfs

    val dfempp= spark.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/employee_personal_details.csv")
    val dfempb= spark.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/employee_business_details.csv")

    // storing temp
    dfempp.createOrReplaceTempView("EmpPe")
    dfempb.createOrReplaceTempView("EmpBu")
    df.createOrReplaceTempView("EmpAd")



    val joinEmp=dfempb.sparkSession.sql("SELECT AVG(Salary) ,MIN(CAST(Salary AS int)) AS min,MAX(CAST(Salary AS int)) AS Max FROM EmpPe FULL OUTER JOIN EmpBu ON EmpPe.Emp_Id = EmpBu.Emp_Id WHERE AgeinYrs BETWEEN 30.00 AND 40.00")
    joinEmp.show()

    val joinEmpYear=dfempb.sparkSession.sql("SELECT Year_of_Joining,COUNT(*) AS No_of_Emp FROM EmpBu GROUP BY Year_of_Joining")
    joinEmpYear.show()

    val joinEmpYearor=    dfempb.sparkSession.sql("SELECT Year_of_Joining,COUNT(*) AS No_of_Emp FROM EmpBu GROUP BY Year_of_Joining ORDER BY No_of_Emp")
    joinEmpYearor.show()

    // salary

    val joinEmpSal=    dfempb.sparkSession.sql("SELECT Emp_Id,Salary AS Current_Salary ,LastHike,CAST((Salary*100)/(REPLACE(LastHike,'%','')+100) AS float) AS salary_befor FROM EmpBu")
    //joinEmpSal.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/EmpSal.csv")
    joinEmpSal.show()

    //avg weight monday....
    val joinEmpAvgW=    dfempb.sparkSession.sql("SELECT AVG(WeightinKgs) FROM EmpPe FULL OUTER JOIN EmpBu ON EmpPe.Emp_Id = EmpBu.Emp_Id  WHERE DOW_of_Joining='Monday' OR DOW_of_Joining='Friday' OR DOW_of_Joining='Wednesday'")
    //joinEmpAvgW.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/Avg_weight.csv")
    joinEmpAvgW.show()


    // date

    val storeHds=dfempb.sparkSession.sql("""SELECT EmpAd.Emp_ID as Emp_ID, First_Name, Last_Name, State, date_format(to_date(DateofBirth, 'M/d/y'), "MM/dd/yyyy") as DOJ FROM EmpBu JOIN EmpAd ON EmpBu.Emp_ID = EmpAd.Emp_ID JOIN EmpPe ON EmpAd.Emp_ID = EmpPe.Emp_ID WHERE State='AK' AND to_date(DateofBirth, 'M/d/y') > to_date('01/01/1980', 'M/d/y')""")
    //storeHds.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/Date_with_AK.csv")
    storeHds.show()
    // max weight

   val mostEmp = dfempb.sparkSession.sql("SELECT State,COUNT(*) AS No_of_Emp FROM EmpAd GROUP BY State ORDER BY No_of_Emp DESC LIMIT 1")
    //mostEmp.write.csv("hdfs://localhost:9000/user/hadoop/datasets/employee/Employee/MostEmp.csv")
  mostEmp.createOrReplaceTempView("MostEMP")
    val joinEmpAvgWS=    dfempb.sparkSession.sql("SELECT MAX(WeightinKgs) FROM EmpPe FULL OUTER JOIN EmpAd ON EmpPe.Emp_Id = EmpAd.Emp_Id WHERE EmpAd.State=(SELECT State FROM MostEMP LIMIT 1) ")
    joinEmpAvgWS.show()


  }
}



