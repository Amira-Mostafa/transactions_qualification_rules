package rules
import java.sql.{Connection, DriverManager}

object db_connection {

  val DB_URL      = "jdbc:mysql://localhost:3306/orders_db?useSSL=false&serverTimezone=UTC"
  val DB_USER     = "amira"
  val DB_PASSWORD = "amira"

  lazy val connection: Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
  }
}

