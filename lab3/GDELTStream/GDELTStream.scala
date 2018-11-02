package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConversions._
// Creating an in-memory key-value store:
// here, we create a `KeyValueStore<String, Long>` named "inmemory-counts".
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.Cancellable

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")
  // Using a `KeyValueStoreBuilder` to build a `KeyValueStore`.

  val allNames:KStream[String, String] = records.map((k,s)=> (k,s.split("\t"))) //[String, Array[String]]
                                                .filter((_,a)=>a.size>23 && a(23)!="") // [String, Array[String]]
                                                .map((k,a)=>(k, a(23).split(";").map(x=>x.split(",")(0))
                                                                                .distinct
                                                                                .mkString(","))) //[String, String]
                                                .flatMapValues(value => value.toLowerCase.split(",")) //[String, String]
  // Publish to topic allNames
  allNames.to("allNames")


  //Initializing the state store
  val keyValueStoreBuilder: StoreBuilder[KeyValueStore[String,Long]] = 
                Stores.keyValueStoreBuilder(
                  Stores.persistentKeyValueStore("countstore"),
                  Serdes.String,
                  Serdes.Long);
  // register store
  builder.addStateStore(keyValueStoreBuilder);

  val outstream: KStream[String, Long] = allNames.transform(new HistogramTransformer, "countstore");
  outstream.to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, Long] = _
  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.kvStore = context.getStateStore("countstore").asInstanceOf[KeyValueStore[String, Long]];
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    val cnt = incrementCount(name) // Increment count for the name
    var scheduled: Cancellable = null
    scheduled = this.context.schedule(60 * 1000, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {
      decrementCount(name) // Decrement count for the name after an hour!
      scheduled.cancel() // We want the count to be decremented only once!
    });
    return (name, cnt)
  }

  def incrementCount(name: String): Long = {
    var count = this.kvStore.get(name)
    if (count != null) {
      count = count + 1
    } else {
      count = 1L
    }
    this.kvStore.put(name, count)
    //println("IncCount", name, count)
    return count
  }

  def decrementCount(name: String) = {
    var count = this.kvStore.get(name)
    if (count == null || count == 1) {
      count = 0
      this.kvStore.delete(name)
    } else {
      count = count - 1
    }
    if (count >= 0) {
      this.kvStore.put(name, count)
    }
    this.context.forward(name, count)
    //println("decCount", name, count)
  }

  // Close any resources if any
  def close() {
  }
}
