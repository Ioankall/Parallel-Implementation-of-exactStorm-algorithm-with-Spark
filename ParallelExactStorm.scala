package Structured_stream

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Structured_stream {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val parallelism = 10 // Parallelise task in 10 workers
  val W = 15000 // 15 seconds window size

  val NEIGHBORHOOD_RANGE = 1 // Distance from point considered neighborhood
  val MINIMUM_ALLOWED_NEIGHBOURS = 10 // If there are less than 10 neighbours, it is an outlier

  //Case class for stream elements
  case class Point(id: Int,
                   modulo: Int,
                   value: Double,
                   flag: Boolean,
                   time: Timestamp,
                   var nn_before: ListBuffer[Timestamp], // For the final implementation we only need the timestamp. Adding a double to keep the value for testing purposes.
                   var count_after: Int,
                   var safe: Boolean)

  //Encoder for case class
  val PointEncoder = Encoders.bean(classOf[Point])


  def main(args: Array[String]): Unit = {

    //Create spark config and session
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OutlierDetection")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder.appName("OutlierDetection").getOrCreate()

    //Import implicits for case class
    import spark.sqlContext.implicits._

    //Random generator
    val r = scala.util.Random

    //Start reading stream
    val lines = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "10")
      .load()
      //Replicate elements to each partition
      .flatMap(p => {
      val id = p.getLong(1).toInt
      val time = p.getTimestamp(0)
      val value = 90 + r.nextInt(19) + r.nextDouble()
      var points = ListBuffer[Point]()
      for (i <- 0 until parallelism) { // create copies of points, one for each worker
        var flag = true
        if (id % parallelism == i) flag = false // only one worker should check this point
        points += new Point(id, i, value, flag, time, ListBuffer(), 0, false)
      }
      points
    })
      //Local aggregate
      .groupByKey(_.modulo) // groups by modulo to send each copy of point to a worker
      .flatMapGroupsWithState(outputMode = OutputMode.Append(), timeoutConf = GroupStateTimeout.NoTimeout)(localAggregate)
      //Global aggregate
      .groupByKey(_.id % parallelism) // groups by id to send all the copies of the same point to the same worker.
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
      timeoutConf = GroupStateTimeout.NoTimeout)(globalAggregate)

    val query = lines.writeStream
      .outputMode("append")
      .option("truncate", false)
      .option("numRows", 200)
      .format("console")
      .start()

    query.awaitTermination()
  }

  def localAggregate(key: Int, input: Iterator[Point], oldState: GroupState[ListBuffer[Point]]): Iterator[Point] = {

    //Evict older elements and elements with flag = 1 from previous calculations

    val inputList = input.toList // The batch with the new elements: some with flag 0 and some with 1
    val endTime = inputList.map(_.time.getTime).max(Ordering.Long) // Time of the latest point created

    var state: ListBuffer[Point] = if (oldState.exists)
      oldState.get.filter(p => !p.flag && p.time.getTime >= (endTime - W))
    else
      ListBuffer[Point]()

    //Compute local aggregates (distances)
    for (el <- inputList) {
      val point = el

      // Increase count_after of items already in state
      state = state.map(p => {
        if (!p.safe && Math.abs(p.value - point.value) < NEIGHBORHOOD_RANGE) {
          p.count_after += 1
          if (p.count_after > MINIMUM_ALLOWED_NEIGHBOURS) p.safe = true
        }
        p
      })

      // Append to nn_before for items already in state
      state.filter(p => Math.abs(p.value - point.value) < NEIGHBORHOOD_RANGE).foreach(p => point.nn_before.+=(p.time))

      // For points flagged for this worker - find all neighbours in latest batch
      // then increase count_after and append to nn_before
      if (!point.flag){
        val inputListNeighborhood = inputList.filter(p => Math.abs(p.value - point.value) < NEIGHBORHOOD_RANGE)
        point.count_after += inputListNeighborhood.count(p => p.time.after(point.time))
        inputListNeighborhood.filter(p => p.time.before(point.time)).foreach(p => point.nn_before.+=(p.time))
      }

      if (point.count_after > MINIMUM_ALLOWED_NEIGHBOURS) {
        point.safe = true
      }

      state.+=(point)
    }

    // Keep up to MINIMUM_ALLOWED_NEIGHBOURS latest neighbors
    state.map(p => {
      p.nn_before = p.nn_before.sortWith(_.getTime > _.getTime).take(MINIMUM_ALLOWED_NEIGHBOURS)
      p
    })

    oldState.update(state)
    state.filter(p => !p.safe).toIterator
  }

  def globalAggregate(key: Int, input: Iterator[Point], oldState: GroupState[ListBuffer[Point]]): Iterator[Point] = {
    //Compute global aggregates (combine lists and counts)
    val inputList = input.toList
    val endTime = inputList.map(_.time.getTime).max(Ordering.Long)

    var state: ListBuffer[Point] = if (!oldState.exists)
      ListBuffer[Point]()
    else
      oldState.get.filter(p => p.time.getTime >= (endTime - W))

    state.map(point => {
      if (input.exists(p => p.id == point.id))
        point.count_after = input.find(p => p.id == point.id).get.count_after
      point
    })

    // At this point we actually need to aggregate only nn_before between copies of points:
    // - count_after is incremented only for not flagged items (no need for aggregation)
    // - safe is bounded only to count_after, so info exist only in not flagged items (no need for aggregation)
    // - nn_before is independent from flag: updated after checking state items that are different on each worker. needs to be aggregated.
    inputList
      .groupBy(_.id)
      .foreach(item => {
        val copiesOfPoint = item._2
        if (copiesOfPoint.exists(p => !p.flag)) {
          val point = copiesOfPoint.find(p => !p.flag).get
          copiesOfPoint.filter(p => p.flag).foreach(p => p.nn_before.foreach(t => point.nn_before += t))
          point.nn_before = point.nn_before.sortWith(_.getTime > _.getTime).take(MINIMUM_ALLOWED_NEIGHBOURS)
          state.+=(point)
        }
      })

    for (point <- state) {
        if (point.nn_before.count(p => p.getTime >= (endTime - W)) + point.count_after < MINIMUM_ALLOWED_NEIGHBOURS) {
          println("\nPoint ", point.id, " with value: ", point.value, " is an outlier.")
        }
    }

    oldState.update(state)
    state.toIterator
  }
}
