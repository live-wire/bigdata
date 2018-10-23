## Processing the entire GDelt Dataset :loop:
`Amazon EMR` `Apache Spark` `GDELT`
##### Spark 2.3.1, Scala 2.11.12

###### :bowtie: [Dhruv Batheja](https://github.com/live-wire), [Shivam Miglani](https://github.com/Shivam-Miglani)

---

For this assignment, we were supposed to process the entire GDelt dataset, and generate top-10 discussed
topics for each day. We were able to process the entire dataset using our dataframe implementation in **7 minutes and 50 seconds** using _20 C4-8xLarge instances_ under $2 using spot prices.

#### Tips and tricks 
- Make sure you remove `.config("spark.master", "local")` from your code before you run your code on EMR in _cluster mode_ to avoid any errors.
- You shouldn't run your code on the entire dataset before trying it out on a smaller set first. We checked scaling in terms of both bigger dataset and more cores/computing power to find suitable machines for the cluster.
- How to process only **n** files from the s3 paths ?:
	- If you didn't notice, the s3 paths of the _csv_ files are provided in the [gdeltv2gkg.txt](https://github.com/Tclv/SBD-2018/blob/master/data/gdeltv2gkg.txt).
	- We used **Regular Expressions** to process _10^n_ files at a time. Our s3 path while reading the files looks like this: 
	`sc.textFile("s3n://gdelt-open-data/v2/gkg/" + prepareRangeString(num) + ".csv")`
	(Notice how we used the native path **s3n://** here instead of _s3://_ for faster file access speeds)
	where and our regex preparation function looks like this:
	```
	def prepareRangeString(num: String) : String = {
	    if (num == "*") {
	      return "*"
	    } else if ( num == "10" ) {
	      return "2015021{8,900,901[01]}*"
	    } else if ( num == "100" ) {
	      return "201502{18,19,200[01]}*"
	    } else if ( num == "1000") {
	      return "20150{2,3010,30110}*"
	    } else if ( num == "10000") {
	      return "20150{[12345],60[12],6030[0123456],603070000}*"
	    } else if ( num == "100000") {
	      return "201{[567],8010[01234567],801080,801081[01234567],80108180000}*"
	    }
	    return "*"
  	}
	```
	We passed the argument _num_ as a runtime argument to the spark-submit command making things convenient for us. You could come up with innovative ways of processing n-files at a time on your own too
-  Your RDD implementation might seem faster for processing 10, 100 or even 1000 files. But the powerful Dataframes/Datasets APIs show their strengths when your data is actually `BIG` :bomb: 
![Scaling behaviour RDD vs Dataset implementation](https://live-wire.github.io/sbd/images/scaling.png)
> "Spark has an optimized SQL query engine that can optimize the compute
path as well as provide a more efficient representation of the rows when given a schema." - Lab Manual


- *Did you find implementing aggregations or doing a ranking of counts cumbersome?* In your dataframe's implementation, try to make use of **[Spark SQL’s Window functions](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-windows.html)** to create new views on the schema and
perform aggregations and rankings on top of those views in a concise and efficient manner. This has been highlighted in our Dataset implementation found [here](https://github.com/live-wire/bigdata/blob/master/lab2/src/main/scala/gdelt.scala#L152).

---
#### EMR Configuration tips
_Is your beautiful code not scaling up as you hoped it would ?_
- **Java Heap Out of Memory error** - We found that this error could be caused due to many
reasons like memory leaks, insufficient driver memory, insufficient executor memory, large cache
memory fraction, large shuffle memory fraction etc. Look for the driver node using Ganglia. The driver node would typically have the least amount of CPU usage. (It is chosen randomly by EMR to be the driver for your cluster in **cluster** mode). Since, all of our nodes were C4-8xLarge, we knew the driver had 60GB of memory available at our disposal. We used the config: 
`--driver-memory 40g` in **spark-submit** command. To make the driver use 40GBs insted of the default 1GB.
![Resource Usage difference when increasing driver memory](https://live-wire.github.io/sbd/images/driver.png)
- **maxResultSize** - spark.driver.maxResultSize was set to 1GB by default and was insufficient
for our initial mapping step. Hence, we increased it to 5GB by adding:
`--conf spark.driver.maxResultSize = 5g` in the **spark-submit** command.

---
:chart_with_upwards_trend:
Plot of time taken by code to process vs the number-of-files processed.
![Final implementation](https://live-wire.github.io/sbd/images/finalimpl.png)

Host your final results using [this](https://github.com/Tclv/SBD-2018/tree/master/visualizer/ass1_2) visualizer like we did: [Final Results](https://live-wire.github.io/sbd/visualizer.html) :microphone: :arrow_down:
