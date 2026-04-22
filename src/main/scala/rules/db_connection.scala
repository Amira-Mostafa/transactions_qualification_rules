package rules
import java.sql.{Connection, DriverManager}
/**
 * Object `db_connection` provides a single, lazily-initialized JDBC connection
 * to the MySQL database used to store processed order results.
 *
 * `lazy val` means the connection is NOT created at program startup.
 *   It is only opened the first time `db_connection.connection` is accessed.
 *   This avoids holding an open connection during file reading or rule setup,
 *   and prevents a crash at startup if the DB is temporarily unreachable.
 *
 * A single shared connection is used In the parallel pipeline,cause only the DB insertion step is sequential
 *
 */
object db_connection {

  val DB_URL = "jdbc:mysql://localhost:3306/orders_db?rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC"
  val DB_USER     = "amira"
  val DB_PASSWORD = "amira"

  lazy val connection: Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
  }
}

