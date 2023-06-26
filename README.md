# metrics-aggregator
This project contains the code to run a Spark job that processes a csv file containing time series data and generates aggregates viz. average, minimum and maximum of values for a particular time bucket. The bucket duration is provided as an argument to the program.

Steps to run the program in an IDE:
1. Set the following program arguments:
	- appName: The name given to the Spark job
 	- inputFilePath - The path of the input csv file that contains the time series data. A sample file is available under src/main/resources/data.
	- delimiter: The delimiter used in the csv file for the program to be able to understand the data in it
	- bucketDurationInHours: The bucket duration. This can be any value greater than 1 but less than or equal to 24.
	- outputFilePath: The path where the outout partition file(s) will be written to. This file will contain the aggregated data.
2. Run the class MetricsAggregator as a java application.
3. Check the output files generated at the outputFilePath location. There must one or more part files.

Steps to run the program in a Spark cluster:
1. You may use the submit-job.sh script available under src/main/resources/scripts after updating the relevant parameters especially the jar file location and the arguments to the program. Alternatively, please construct your relevant spark-submit command and launch the job in a Spark cluster.
2. Please also change the scope of spark jars to "provided" and comment out the line number 93 (resource manager url), then compile and finally execute, if running in a Spark cluster.

Important:
1. Minimum Java version required - 1.8
2. Spark version - 3.4.0
3. Scala version using which Spark jars were built - 2.12