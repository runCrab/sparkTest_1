import com.utils.Tag
import org.apache.spark.sql.Row


object TagChannel extends Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]

    val channel =row.getAs[Int]("adplatformproviderid").toString
    list:+=(channel,1)
    list
  }
}
