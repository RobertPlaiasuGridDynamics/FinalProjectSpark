import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Records(positionId: Long, amount: BigDecimal, eventTime: Timestamp)
