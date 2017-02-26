import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by Soumyakanti on 26-02-2017.
  */
object Utilities {

  def main(args: Array[String]): Unit = {
    println(getNextDate("2016-12-31"))
  }

  def getNextDate(date: String): String = {
    val dateFormat = "yyyy-MM-dd"

    val simpleDateFormat = new SimpleDateFormat(dateFormat)
    val oldDate = simpleDateFormat.parse(date)

    val cal = Calendar.getInstance
    cal.setTime(oldDate)
    cal.add(Calendar.DATE, 1)

    val nextDate = cal.getTime
    simpleDateFormat.format(nextDate)
  }

}
