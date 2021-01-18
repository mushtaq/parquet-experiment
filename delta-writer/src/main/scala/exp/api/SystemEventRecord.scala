package exp.api

import java.time.Instant
import java.util.UUID

import exp.data.ParamSetJson

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
  def generate(): SystemEventRecord = generate("0", "startEvent", UUID.randomUUID().toString)

  def generate(exposureId: String, obsEventName: String, eventId: String): SystemEventRecord = {
    val instant = Instant.now()
    SystemEventRecord(
      exposureId,
      obsEventName,
      eventId,
      "wfos.blue.filter",
      "filter wheel",
      instant.toString,
      instant.getEpochSecond,
      instant.getNano,
      ParamSetJson.jsonString
    )
  }
}
