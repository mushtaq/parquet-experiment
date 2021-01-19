package exp.api

import csw.params.core.formats.ParamCodecs._
import csw.params.events.SystemEvent
import io.bullet.borer.derivation.MapBasedCodecs
import io.bullet.borer.{Codec, Json}

case class SystemEventRecord(
    exposureId: String,
    obsEventName: String,
    eventId: String,
    source: String,
    eventName: String,
    eventTime: String,
    seconds: Long,
    nanos: Long,
    paramSet: String
)

object SystemEventRecord {
  def generate(exposureId: Long, obsEventName: String, systemEvent: SystemEvent): SystemEventRecord = {
    SystemEventRecord(
      exposureId.toString,
      obsEventName,
      systemEvent.eventId.id,
      systemEvent.source.toString(),
      systemEvent.eventName.name,
      systemEvent.eventTime.value.toString,
      systemEvent.eventTime.value.getEpochSecond,
      systemEvent.eventTime.value.getNano,
      Json.encode(systemEvent.paramSet).toUtf8String
    )
  }

  implicit lazy val seCodec: Codec[SystemEventRecord] = MapBasedCodecs.deriveCodec
}
