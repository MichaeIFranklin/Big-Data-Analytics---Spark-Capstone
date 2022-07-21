//***************************************************************
//
//  Developer:         Michael Franklin
//
//  Program #:         Capstone
//
//  File Name:         CapstoneSparkDriver.java
//
//  Course:            COSC 3365 – Distributed Databases Using Hadoop 
//
//  Due Date:          05/13/22
//
//  Instructor:        Prof. Fred Kumi 
//
//  Description:
//     Driver class for Apache Spark (Project 10)
//
//***************************************************************

package sparkCapstone;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

// implements Serializable to allow Spark to send the class object 
// (created during runtime) through a stream to the various worker nodes
public class CapstoneSparkDriver implements Serializable {

	final private String InputFilename = "Capstone.txt";
	final private String SeperatorStr = ",";
	
	// set to transient as JavaSpakrContext can't be serialized
	transient private JavaSparkContext sc;
	
	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	public static void main(String[] args) 
	{		
		// initialize program object
		CapstoneSparkDriver obj = new CapstoneSparkDriver();
		obj.developerInfo();
		
		// setup spark configurations and context handler
		obj.Setup();
		
		// read in data
		JavaRDD<String> data = obj.ReadInData(obj.InputFilename);
		
		// map data to values
		JavaPairRDD<String, String> mappedPairRDD = obj.Mapper(data);
		
		// reduce data to sums
		JavaPairRDD<String, Integer> averagesRDD = obj.Reducer(mappedPairRDD);
 
		// print results
		obj.Output(averagesRDD);
  
		// cleanup and close any streams or handles
        obj.Cleanup();
	}
	
	
	//***************************************************************
    //
    //  Method:       Setup
    // 
    //  Description:  sets up various configurations and the spark 
	//				  context handle
    //
    //  Parameters:   None
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	private void Setup()
	{
		// suppresses most warnings from apache
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// set app name and set spark to local mode
		SparkConf conf = new SparkConf().setAppName("adAverager").setMaster("local[*]");
		
		// fixes various annoying errors
		conf.set("spark.driver.bindAddress", "127.0.0.1");
		
		// Initialize Spark context handle
		sc = new JavaSparkContext(conf);
	}
	
	
	//***************************************************************
    //
    //  Method:       ReadInData
    // 
    //  Description:  reads in data from a file
    //
    //  Parameters:   String: filename of file to read from
    //
    //  Returns:      String JavaRDD: data from file 
    //
    //**************************************************************
	private JavaRDD<String> ReadInData(String filename)
	{
		// read in data from file
		JavaRDD<String> rawInputData = sc.textFile(filename);

		return rawInputData;
	}
	
	
	//***************************************************************
    //
    //  Method:       Mapper
    // 
    //  Description:  maps each order (per customer), to a string 
	//				  containing if the order was returned and if
	//				  a fraud point was assigned
    //
    //  Parameters:   String JavaRDD: RDD containing data to map
    //
    //  Returns:      String,String JavaPairRDD: mapped data
	//									("num-name", "returned,point")
    //
    //**************************************************************
	private JavaPairRDD<String, String> Mapper(JavaRDD<String> data)
	{
		
		return data.mapToPair(lineData -> {
			// get items in line
			String[] items = lineData.split(SeperatorStr);
			
			// create complex key
			String key = items[0] + "-" + items[1];
			
			// check if returned
			int point = 0;
			boolean returned = items[6].equals("yes");
			
			// get return data if returned
			if (returned)
			{
				// parse received and returned dates
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
			    Date recvDate = dateFormat.parse(items[5]);
			    Date retnDate = dateFormat.parse(items[7]);

			    long differenceInMs = retnDate.getTime() - recvDate.getTime();
			    long differenceInDays = TimeUnit.DAYS.convert(differenceInMs, TimeUnit.MILLISECONDS);
			    
			    // check if day difference is greater than 10
			    if (differenceInDays > 10)
			    {
			    	// assign fraud point
			    	point = 1;
			    }
			}

			// return tuple
			return new Tuple2<>(key, (returned ? "1" : "0")
					+ "," + point);
		});
	}
	
	
	//***************************************************************
    //
    //  Method:       Reducer
    // 
    //  Description:  reduces data by finding the return rate of all 
	//				  orders belonging to a customer and finalizing
	//				  total fraud points
    //
    //  Parameters:   String,String JavaPairRDD: RDD containing mapped
	//				  data to reduce
    //
    //  Returns:      String,Integer JavaPairRDD: reduced data
    //
    //**************************************************************
	private JavaPairRDD<String, Integer> Reducer(JavaPairRDD<String, String> mappedData)
	{
		// reduce each matching key by finding the average of each rate		
		return mappedData.mapValues(v ->
		{
			String[] strs = v.split(",");
			int returned = Integer.parseInt(strs[0]);
			int point = Integer.parseInt(strs[1]);
			return new Tuple3<>(returned,point,1);
		})
		.reduceByKey((value1, value2) -> new Tuple3<>(value1._1() + value2._1(), value1._2() + value2._2(), value1._3() + value2._3()))
		.mapToPair(record -> {
			
			// get the return rate
			double returnRate = (record._2._1() * 1.0) / record._2._3() * 100;
			
			// calculate final fraud points
			Integer points = record._2._2();
			if (returnRate >= 50)
			{
				// assign 10 additional fraud points
				points += 10;
			}
			
			// format the key
			String[] labels = record._1.split("-");
			String label = String.format("%-20s%-20s",
					labels[0],labels[1]);
			
			
			return new Tuple2<>(label, points);
		});
	}
	
	
	//***************************************************************
    //
    //  Method:       Output
    // 
    //  Description:  sorts by key and outputs records in passed PairRDD
    //
    //  Parameters:   String,Integer JavaPairRDD: RDD to output
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	private void Output(JavaPairRDD<String, Integer> reducedData)
	{
		// flip keys and values to sort by value
		JavaPairRDD<Integer, String> sorterRDD = reducedData.mapToPair(
				record -> new Tuple2<>(record._2,record._1));
		
		// sort by key
		sorterRDD = sorterRDD.sortByKey(false, 1);
		
		// flip keys and values for output
		JavaPairRDD<String, Integer> outputRDD = sorterRDD.mapToPair(
						record -> new Tuple2<>(record._2,record._1));
		
		// print out labels
		System.out.printf("%-20s%-20s%-20s\n",
				"Customer #","Customer Name","Fraud Points");
		
		// print out results 
		outputRDD.foreach(tuple -> System.out.println(tuple._1 + String.format("%-5s%s","" + tuple._2, "Pts")));
	}
	
	
	//***************************************************************
    //
    //  Method:       Cleanup
    // 
    //  Description:  closes any open streams and handles
    //
    //  Parameters:   None
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	private void Cleanup()
	{
		// close context handle
		sc.close();
	}
	
	
	//***************************************************************
	//
	//  Method:       developerInfo
	// 
	//  Description:  The developer information method of the program
	//
	//  Parameters:   None
	//
	//  Returns:      N/A 
	//
	//**************************************************************
	public void developerInfo()
	{
		System.out.println("Name:    Michael Franklin");
		System.out.println("Course:  COSC 3365 Distributed Databases Using Hadoop");
		System.out.println("Project: Capstone\n");

	}
}
