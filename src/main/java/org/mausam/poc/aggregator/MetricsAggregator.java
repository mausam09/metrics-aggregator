package org.mausam.poc.aggregator;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


/**
 * This class contains the code to run a Spark job that processes a csv file containing time series data 
 * and generates aggregates viz. average, minimum and maximum of values for a particular time bucket. The 
 * bucket duration is provided as an argument to the program.
 * The input arguments (listed below) needed by this job are provided as a whitespace separated list.
 * - appName: The name given to the Spark job
 * - inputFilePath - The path of the input csv file that contains the time series data
 * - delimiter: The delimiter used in the csv file for the program to be able to understand the data in it
 * - bucketDurationInHours: The bucket duration. This can be any value greater than 1 but less than or equal to 24.
 * - outputFilePath: The path where the outout partition file(s) will be written to. This file will contain the aggregated data.
 * 
 * The following steps are executed by the program:
 * Step 1 - Checks if all arguments have been provided and none of them are blank. Terminates the job otherwise.
 * Step 2 - Extracts the arguments into a set of local variables for easier readability
 * Step 3 - Checks if the bucket duration is between 1 and 24. Terminates the job otherwise.
 * Step 4 - Constructs the spark session
 * Step 5 - Reads the file into a Dataset and adds the following two columns:
 * 			- Date: This is the date part of the timestamp column. This is required for grouping based on metric type, 
 * 			  date and the time bucket at a later stage.
 *          - Bucket: This is the bucket to which the record belongs. For example, if the user has provided 4 as the 
 *            bucketDurationInHours, all records for a particular day will be categorized into 6 buckets (24 hours divided by 4). 
 *            The bucket id starts with zero.
 * Step 6 - This is where the grouping by the metric, date and the bucket id happens, then the aggregate values viz. average, min 
 * 			and max are calculated. The subsequent order by is simply for better readability of the output.
 * Step 7 - Write the output as a csv file at the given location. Csv has been chosen for easy visualization of the output. The 
 * 			SaveMode should be reconsidered based on the nature of data, the schedule of job runs, whether output partitioning based 
 * 			on date is in place, amongst other factors.
 * Step 8 - Close spark session.
 * 
 * Note: It is suggested that apart from the bucket id column, the output also contains the actual time range column. For example, 
 * if the user has provided 6 as the bucketDuration, the day will be split into 4 buckets and the time ranges will be as follows. 
 * However, this requires more time than that mentioned in the coding challenge document (3 to 4 hours). Hence, this can be 
 * considered as a potential improvement.
 * 	Bucket 0: 00:00:00 - 05:59:59
 * 	Bucket 1: 6:00:00 - 11:59:59
 * 	Bucket 2: 12:00:00 - 17:59:59
 * 	Bucket 3: 18:00:00 - 23:59:59
 * Three other areas of improvement are exception handling, using logger instead of System.out.println and using constants instead 
 * of String literals like the column names.
 *
 */
public class MetricsAggregator {

	public static void main(String[] args) {
		// Step 1
		if (args.length < 5 
				|| (args[0] == null && args[0].trim().isEmpty())
				|| (args[1] == null && args[1].trim().isEmpty())
				|| (args[2] == null && args[2].trim().isEmpty())
				|| (args[3] == null && args[3].trim().isEmpty())
				|| (args[4] == null && args[4].trim().isEmpty())) {
			System.out.println("Insufficient arguments provided. Aborting program.");
			System.exit(-1);
		}
		
		// Step 2
		String appName = args[0].trim();
		String inputFilePath = args[1].trim();
		String delimiter = args[2].trim();
		int bucketDurationInHours = Integer.valueOf(args[3].trim());
		String outputFilePath = args[4].trim();
		
		// Step 3
		if (bucketDurationInHours < 1 || bucketDurationInHours > 24) {
			System.out.println("Arguments bucket duration must be between 1 and 24 (both inclusive). Aborting program.");
			System.exit(-1);
		}
		
		System.out.println(String.format("Starting job '%s' with input file(s) at '%s'"
				+ ", delimiter '%s', bucket duration '%s hours' and output file(s) at '%s'", 
				appName, inputFilePath, delimiter, bucketDurationInHours, outputFilePath));
		
		// Step 4
		SparkSession sparkSession = SparkSession
				.builder()
				.appName(appName)
				.master("local[*]")
				.config("spark.sql.session.timeZone", "UTC")
				.getOrCreate();
		
		// Step 5
		Dataset<Row> df = sparkSession
				.read()
				.option("inferSchema", true)
				.option("header", true)
				.option("delimiter", delimiter)
				.csv(inputFilePath)
				.withColumn("Date", to_date(col("Timestamp")))
				.withColumn("Bucket", floor(hour(col("Timestamp")).divide(bucketDurationInHours)));
		
		// Step 6
		df = df.groupBy(col("Metric"), col("Date"), col("Bucket"))
				.agg(avg("Value").as("Average"), min("Value").as("Min"), max("Value").as("Max"))
				.orderBy(col("Metric"), col("Date"), col("Bucket"));
		
		// Step 7
		df.write()
			.option("header", true)
			.mode(SaveMode.Overwrite)
			.csv(outputFilePath);
				
		// Step 8
		sparkSession.close();
		
		System.out.println(String.format("Job %s completed successfully.", appName));
	}

}
