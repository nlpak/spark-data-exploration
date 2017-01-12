import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
// case class to apply sechema to data frames 
case class Page(l_code:String, p_title:  String, vcount: Long, p_size: Long) 
object Pagecounts 
{
		
	def main(args: Array[String]) 
	{
		
		val spark = SparkSession.builder.
      		master("local")
      		.appName("spark session example")
      		.getOrCreate()	// Gets Language's name from its code
		def getLangName(code: String) : String =
		{     
			return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
		}
	    import spark.implicits._

		val inputFile = args(0)  // Get input file's name from this command line argument
        System.out.println("Reading input file ... ")
       
       val pagecounts = spark.sparkContext.textFile(inputFile)
//     Reading file line by line and doing following in single line
//     1- splitting the words by space, 
//     2- Remving the string after . in language code 
//     3- Maping a Page class that I defined above to fice structure to my DataFrame 
//     3- convering RDD to DataFrame 
		val df = pagecounts.map(_.split(" ")).map(attributes => Page(StringUtils.substringBefore(attributes(0), "."), attributes(1), attributes(2).toLong, attributes(3).toLong) ).toDF
		   //now df is talbe below
		
		System.out.println("Filtering dataFrame ... ")
		//Filtering lines having same page title and language code 
		val filter_df = df.filter(line=> line(0) !=line(1))
		// Registring it as table pageRecordTBL
		filter_df.createOrReplaceTempView("pageRecordTBL")
		//	Data Frame with Groupby languge code with sum of views per language
		System.out.println("Creating sum table  ... ")
		val pageViewSumDF = spark.sql("SELECT l_code, SUM(vcount) as sum FROM pageRecordTBL GROUP BY l_code")
		// will be accessed as sumTBL table 
		pageViewSumDF.createOrReplaceTempView("sumTBL")
		//table with max views per language
		val maxDF = spark.sql("SELECT l_code,  MAX(vcount) as max FROM pageRecordTBL GROUP BY l_code") 
		// max table, 
		maxDF.createOrReplaceTempView("maxTBL")
    // Above was intermediatet transformation to prepair helping data,  
    
		// selecting required attributes from the pageRecordTBL table
    	val queryStr = "SELECT pageRecordTBL.l_code as l_code, pageRecordTBL.p_title as p_title, pageRecordTBL.vcount as vcount FROM pageRecordTBL INNER JOIN maxTBL ON pageRecordTBL.l_code=maxTBL.l_code WHERE pageRecordTBL.vcount=maxTBL.max AND pageRecordTBL.l_code=maxTBL.l_code"
		val  urlDF = spark.sql(queryStr)
		// now it will be accessible as talbe url 	
		urlDF.createOrReplaceTempView("urlTBL") 
		
		System.out.println("Joining two temp Views  ... ")

		// (Language-code, totalViewsinLang, page-title, viwesofthatLang)
		val resultDF = spark.sql("SELECT a.l_code, a.sum, c.p_title, c.vcount FROM sumTBL a INNER JOIN urlTBL c ON a.l_code=c.l_code")
    
		
		// Final sort on data desc in tottal views per language  
		val sortedDF =  resultDF.sort(desc("sum"))  //orderBy(desc("sum"))
        val outputRDD = sortedDF.rdd.map{ rdd => ( rdd(0), getLangName(rdd(0).toString), rdd(1), rdd(2), rdd(3)) }
        
        // val outputRDD = resultDF.rdd.map{ rdd => ( rdd(0), getLangName(rdd(0).toString), rdd(1), rdd(2), rdd(3)) }

		// outputRDD.coalesce(1).write.csv("/home/najeeb/spark/wksp/pro_out4")
        System.out.println("Writting Output file ... ") 

		// d.orderBy(desc("sum")).coalesce(1).write.csv("/home/najeeb/spark/wksp/pro_out")

		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
	
	    // writting output
	    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("PageCounts.txt"), "UTF-8"))
	    outputRDD.collect().foreach( x => bw.write(x.toString()+ "\n")  ) 

        bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.out.println("Hello this is my first line print as Spark Programmer, May God Help me to get success in this domain. ! IA")
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}
