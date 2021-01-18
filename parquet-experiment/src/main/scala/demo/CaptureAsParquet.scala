package demo

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.github.mjakubowski84.parquet4s.ParquetWriter
import exp.api.{Constants, EventServiceMock}
import exp.jobs.ParquetIO
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object CaptureAsParquet {
  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY
  )

  def main(args: Array[String]): Unit = {
    implicit lazy val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "demo")
    import actorSystem.executionContext

    val parquetIO = new ParquetIO(Constants.BatchingDir)

    EventServiceMock
      .eventStream()
      .groupedWithin(10000, 5.seconds)
      .mapAsync(1) { batch =>
        parquetIO.write(batch).map(_ => batch.length)
      }
      .statefulMapConcat { () =>
        var start = System.currentTimeMillis()
        batchSize =>
          val current = System.currentTimeMillis()
          println(s"Finished writing batch size $batchSize in ${current - start} milliseconds >>>>>>>>>>>>>>>>>>>>")
          start = current
          List(batchSize)
      }
      .run()
      .onComplete { x =>
        actorSystem.terminate()
        x match {
          case Failure(exception) => exception.printStackTrace()
          case Success(value)     =>
        }
      }
  }
}
