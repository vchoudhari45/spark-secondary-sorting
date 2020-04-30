package com.vc.model

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.slf4j.LoggerFactory
import scala.util.Try

case class UserActivity(userId: String, website: String, visitedTimestamp: Int)
case class FailSafeRow[T](row: Option[T])

object FailSafeUserActivity {
  private[this] val log = LoggerFactory.getLogger(FailSafeUserActivity.getClass)
  implicit val failSafeUserActivity: Encoder[FailSafeRow[UserActivity]] = Encoders.product[FailSafeRow[UserActivity]]
  implicit val userActivity: Encoder[UserActivity] = Encoders.product[UserActivity]
  implicit val stringEncoder: Encoder[String] = Encoders.STRING

  def csvToUserActivity(row: Row): FailSafeRow[UserActivity] = {
    Try {
      val userId = row.getString(0).trim
      val website = row.getString(1).trim
      val timestampStr = row.getString(2).trim
      val timestamp = (timestampStr.substring(0, 2) + timestampStr.substring(3, 5) + timestampStr.substring(6)).toInt
      UserActivity(userId, website, timestamp)
    }.fold(ex => {
      //don't fail spark executor on parsing error, log the error and continue
      log.error(s"parsing error row '$row' is not valid")
      FailSafeRow(None)
    }, o => FailSafeRow(Some(o)))
  }

  def userActivityToCsv(row: Row): String = {
    val userId = row.getString(0)
    val website = row.getString(1)
    val currentTime = row.getInt(2).toString
    val nextTime = row.getInt(3).toString
    val formattedCurrentTime = s"${currentTime.substring(0, 2)}:${currentTime.substring(2, 4)}:${currentTime.substring(4)}"
    if (nextTime.toInt == 0) s"$userId, $website, $formattedCurrentTime, NULL"
    else {
      val hhDiff = (nextTime.substring(0, 2).toInt - currentTime.substring(0, 2).toInt) * 3600
      val mmDiff = (nextTime.substring(2, 4).toInt - currentTime.substring(2, 4).toInt) * 60
      val ssDiff = nextTime.substring(4).toInt - currentTime.substring(4).toInt
      val total = hhDiff + mmDiff + ssDiff
      s"${row.getString(0)}, ${row.getString(1)}, $formattedCurrentTime, ${total}s"
    }
  }
}