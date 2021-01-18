package exp.jobs

import akka.Done
import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s._
import exp.api.SystemEventRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}

class ParquetIO(path: String)(implicit actorSystem: ActorSystem[_]) {

  private val blockingEC: ExecutionContextExecutor = actorSystem.dispatchers.lookup(DispatcherSelector.blocking())

  private val writeOptions: ParquetWriter.Options = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  def writeAsStream(batch: Seq[SystemEventRecord]): Future[Done] = {
    Source(batch)
      .via(
        ParquetStreams
          .viaParquet[SystemEventRecord](path)
          .withWriteOptions(writeOptions)
          .withPartitionBy("exposureId", "obsEventName")
          .build()
      )
      .run()
  }

  def write(batch: Seq[SystemEventRecord]): Future[Path] =
    Future {
      val uuid             = UUID.randomUUID().toString
      val tmpLocation      = s"/tmp/parquet/$uuid.parquet"
      val tmpCrcLocation   = s"/tmp/parquet/.$uuid.parquet.crc"
      val finalLocation    = s"$path/$uuid.parquet"
      val finalCrcLocation = s"$path/.$uuid.parquet.crc"
      val w                = ParquetWriter.writer[SystemEventRecord](tmpLocation)
      w.write(batch)
      w.close()
      Files.move(Paths.get(tmpCrcLocation), Paths.get(finalCrcLocation))
      Files.move(Paths.get(tmpLocation), Paths.get(finalLocation))
    }(blockingEC)

  def read[P](exposureId: String)(implicit schemaResolver: ParquetSchemaResolver[P], decoder: ParquetRecordDecoder[P]): Future[Seq[P]] = {
    read(Col("exposureId") === exposureId)
  }

  def read[P](exposureId: String, obsEventName: String)(implicit
      schemaResolver: ParquetSchemaResolver[P],
      decoder: ParquetRecordDecoder[P]
  ): Future[Seq[P]] = {
    read(Col("exposureId") === exposureId && Col("obsEventName") === obsEventName)
  }

  def read[P](filter: Filter)(implicit schemaResolver: ParquetSchemaResolver[P], decoder: ParquetRecordDecoder[P]): Future[Seq[P]] = {
    ParquetStreams
      .fromParquet[P]
      .withProjection
      .withFilter(filter)
      .read(path)
      .runWith(Sink.seq)
  }
}
