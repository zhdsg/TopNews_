import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by admin on 2018/6/19.
 */
object KafkaUtils1{
  val TOPIC ="zhanghao"
  def getProducer():KafkaProducer[String,String]={
    val props :Properties = new  Properties()
    val propsMap  =Map[String,String](
    "bootstrap.servers"->"hadoop-sh1-core1:9092",
    "acks"->"all",
    "retries"->"0",
    "batch.size"->"16384",
    "linger.ms"->"1",
    "buffer.memory"->"33554432",
    "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer"
    )
    props.putAll(propsMap)
    new KafkaProducer[String,String](props)
  }
  def main(args: Array[String]) {
    val producer =KafkaUtils1.getProducer()
    val chars =Array[String]("a","b"," ","c")
    val random =new Random()
//    val order = new Order("zhanghao",22,List[String]("aaa","bbbb").asJava)
    for( i <- 1 to 100000){
      var word =""
      for(j <- 0 to random.nextInt(9))
      {

          word =chars(random.nextInt(3)) + word
      }
      println(word)
      //println(i%10)
      val record =new ProducerRecord[String,String](TOPIC,word)
      producer.send(record)

      if( i % 10 == 0 ){
        Thread.sleep(1000)
      }

      Thread.sleep(100)
    }

    producer.close()


  }
}