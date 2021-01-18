package exp.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import csw.EventFactory

import scala.concurrent.duration.DurationInt

object EventServiceMock {
  def eventStream(): Source[SystemEventRecord, NotUsed] = {
    val eventIds  = Iterator.from(1)
    val exposures =
      Iterator.from(1).flatMap { exposureId =>
        Iterator("startEvent", "endEvent").flatMap { obsEventName =>
          List.fill(22000)((exposureId, obsEventName))
        }
      }

    Source
      .fromIterator(() => exposures.zip(eventIds))
//      .throttle(10, 1.millis)
      .map {
        case ((exposureId, obsEventName), eventId) =>
          SystemEventRecord.generate(exposureId, obsEventName, EventFactory.generateEvent(eventId))
      }
      .take(200000)
  }

  def captureSnapshot(exposureId: Long, obsEventName: String): Seq[SystemEventRecord] = {
    (1 to 2300).map(_ => SystemEventRecord.generate(exposureId, obsEventName, EventFactory.generateEvent()))
  }

  def captureSnapshot2(exposureId: Long, obsEventName: String): Seq[SystemEventRecord2] = {
    (1 to 2300).map(_ => SystemEventRecord2.generate(exposureId, obsEventName, EventFactory.generateEvent()))
  }
}
