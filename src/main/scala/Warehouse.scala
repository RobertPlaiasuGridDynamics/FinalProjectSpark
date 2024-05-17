import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Warehouse(positionId: Long,warehouse: String, product: String, eventTime: Timestamp  )
