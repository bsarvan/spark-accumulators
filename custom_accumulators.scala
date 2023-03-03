import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.Map


class Models extends Serializable with Logging {
  var models = Map[String, String]()

  def addModel(path: String, apiID: String): Unit = {
    models.put(path, apiID)
  }

  def add(m1: Models): Unit = {
    if (models == null) {
      models = m1.models
    } else {
      this.merge(m1)
    }
  }

  def merge(m1: Models): Unit = {
    m1.models.foreach(elem => {
      if (this.models.contains(elem._1)) {
        this.models(elem._1) = elem._2
      } else {
        this.models.put(elem._1, elem._2)
      }
    })
  }
}

class ModelsAccumulator(models: Models) extends AccumulatorV2[Models, Models] {
  override def add(other: Models): Unit = {
    println("ModelsAccumulator Add Method")
    models.add(other)
  }

  override def copy():ModelsAccumulator = {
    println("ModelsAccumulator Copy Method")
    new ModelsAccumulator(models)
  }

  override def value(): Models = {
    println("ModelsAccumulator Value Method")
    return models
  }

  override def isZero: Boolean = {
    return models.models.isEmpty
  }

  override def merge(other: AccumulatorV2[Models, Models]): Unit = {
    println("ModelsAccumulator Merge Method")
    models.merge(other.value)
  }

  override def reset(): Unit = {
    models.models.clear()
  }
}

object Main {
  var modelAccum: ModelsAccumulator = _

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local[5]")
      .getOrCreate()

    var models = new Models()
    modelAccum = new ModelsAccumulator(models)
    spark.sparkContext.register(modelAccum, "My Models Accumulator")

    var df = spark.read.format("avro").load(("gamora.avro"))

    df.foreach(row => BuildModel(row))

    println(s"Finished Process - Models Built ${modelAccum.value().models.size}")
    spark.stop()
  }

  def BuildModel(row: Row): Unit = {
    var path = row.getAs("path").toString
    var apiID = row.getAs("api_id").toString

    var newModels = new Models()
    newModels.addModel(path, apiID)
    modelAccum.add(newModels)
  }
}
