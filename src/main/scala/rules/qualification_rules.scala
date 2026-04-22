package rules

import scala.io.Source
import java.time.{LocalDate, ZonedDateTime}
import java.time.temporal.ChronoUnit
import scala.util.Using
import rules.logger.logger
import rules.Order

/**
 * Object `rules` implements an order processing pipeline that:
 * - Reads raw order data from a CSV file.
 * - Parses each CSV line into an Order case class.
 * - Defines a set of discount rules with qualifying conditions and discount calculations.
 * - Processes each order to calculate total price before discount,
 *   determines applicable discounts, averages the top two discounts,
 *   and calculates the final price after discounts.
 * - Inserts the processed order results into a database table.
 * - Logs key pipeline stages and handles errors gracefully.
 *
 * The pipeline steps:
 * 1. Read input CSV `TRX1000.csv` and skip the header.
 * 2. Define Order class representing the input data schema.
 * 3. Define various discount rules, each with:
 *    - A Boolean predicate function to check if an order qualifies, if true it qualifies if false it's not.
 *    - A discount function returning the discount amount.
 * 4. Parse each line to an Order object.
 * 5. For each order:
 *    - Calculate total price before discount.
 *    - Collect all discounts that apply to the order.
 *    - Pick the top two highest discounts and average them.
 *    - Calculate the final total price after applying the average discount.
 * 6. Insert all order data along with totals and discount into my sql DB.
 * 7. Log successes and errors.
 *
 */


object qualification_rules extends App {
  try {
    logger.info("Starting order processing pipeline...")

    /**
     * Reads CSV lines from file, skipping the header line.
     * @return List of raw CSV lines (String) excluding the header with drop(1)
     */
    val lines: List[String] = Using.resource(Source.fromFile("src/main/resources/TRX1000.csv")) { source =>
      source.getLines().drop(1).toList
    }
//    val lines: List[String] = Using.resource(Source.fromFile("src/main/resources/TRX10M.csv")) { source =>
//      source.getLines().drop(1).toList
//    }

    logger.info(s"Extracted ${lines.size} orders from CSV.")


    /**
     * Type alias for qualifying condition function for discount rules.
     * Takes an Order and returns Boolean indicating if discount applies.
     */
    type BoolFunc = Order => Boolean

    /**
     * Type alias for discount calculation function.
     * Takes an Order and returns a Double discount value.
     */
    type DiscountFunc = Order => Double

    /**
     * Map of product-based discounts for specific products.
     */
    val product_discountMap: Map[String, Double] = Map("cheese" -> 0.10, "wine" -> 0.05)

    /**
     * Map of quantity ranges to discounts.
     * Each key is a tuple (minQuantity, maxQuantity).
     */
    val quantity_discountMap: Map[(Int, Int), Double] = Map(
      (6, 9) -> 0.05,
      (10, 14) -> 0.07,
      (15, Int.MaxValue) -> 0.10
    )

    /**
     * Parses a each CSV line into an Order instance.
     * @param line CSV line string.
     * @throws IllegalArgumentException if line does not have exactly 7 columns.
     * @return Order instance.
     */
    def to_order(line: String): Order = {
      val parts = line.split(",")
      if (parts.length != 7)
        throw new IllegalArgumentException(s"Invalid line format: $line")
      Order(parts(0), parts(1), parts(2), parts(3).toInt,
        parts(4).toDouble, parts(5), parts(6))
    }

    /**
     * Extracts date portion (yyyy-MM-dd) from ISO timestamp string.
     * @param d Timestamp string in ISO format.
     * @return Date portion string.
     */
    def process_date(d: String): String = d.split('T')(0)

    /**
     * Extracts day as integer from date string (yyyy-MM-dd).
     * @param d Date string.
     * @return Day of month integer.
     */
    def extract_day(d: String): Int = d.split('-')(2).toInt

    /**
     * Extracts month as integer from date string (yyyy-MM-dd).
     * @param d Date string.
     * @return Month integer.
     */
    def extract_month(d: String): Int = d.split('-')(1).toInt

    /**
     * Parses date string into LocalDate.
     * @param d Date string.
     * @return LocalDate object.
     */
    def toDate(d: String): LocalDate = LocalDate.parse(d)

    /**
     * Calculates number of days between two dates.
     * @param startDate Start LocalDate.
     * @param endDate   End LocalDate.
     * @return Days between startDate and endDate as Long.
     */
    def calc_days(startDate: LocalDate, endDate: LocalDate): Long =
      ChronoUnit.DAYS.between(startDate, endDate)

    // -------------------------- Discount qualifying and calculation functions ------------------------------

    /**
     * Condition 1: Order expiry date is less than 30 days after order timestamp and greater than the exact expiry day
     */
    val is_less30: BoolFunc = order => {
      val moreThan30 = calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date))
      moreThan30 > 0 && moreThan30 < 30
    }
    /**
     * Discount based on how close to expiry the order is, scaled between 0 and 0.30.
     */
    val less30_Discount: DiscountFunc = order => {
      val rem_days = calc_days(toDate(process_date(order.timestamp)), toDate(order.expiry_date))
      (30 - rem_days) / 100.0
    }

    /**
     * Condition 2: Order product is in product_discountMap (e.g., cheese or wine).
     */
    val is_chess_wine: BoolFunc = order =>
      order.product_name.toLowerCase.contains("cheese") ||
        order.product_name.toLowerCase.contains("wine")
    /**
     * Discount amount for cheese or wine products based on unit price.
     */
    val chee_wine_Discount: DiscountFunc = order =>
      product_discountMap.getOrElse(order.product_name, 0.0)


    /**
     * Condition 3: Order was sold specifically on March 23rd.
     */
    val is_sold23March: BoolFunc = order => {
      val day = extract_day(process_date(order.timestamp))
      val month = extract_month(process_date(order.timestamp))
      day == 23 && month == 3
    }
    /**
     * Fixed 50% discount if order sold on March 23rd.
     */
    val sold23March_Discount: DiscountFunc = _ => 0.5

    /**
     * Condition 4: Order quantity greater than 5.
     */
    val is_more5: BoolFunc = order => order.quantity > 5

    /**
     * Discount based on quantity ranges.
     */
    val more5_Discount: DiscountFunc = order => {
      val qty = order.quantity
      quantity_discountMap.collectFirst {
        case ((min, max), discount) if qty >= min && qty <= max => discount
      }.getOrElse(0.0)
    }
//    val quantityRule: DiscountFunc = (
//      order => order.quantity >= 6,
//      // according to the quantity sold will apply a specific discount
//      order => order.quantity match {
//        case q if q >= 6 && q <= 9 => 5.0
//        case q if q >= 10 && q <= 14 => 7.0
//        case q if q >= 15 => 10.0
//        case _ => 0.0
//      }
//    )

    /**
     * Condition 5: Order placed through the "App" channel column.
     */
    val is_app: BoolFunc = order => order.channel == "App"

    /**
     * Discount based on app orders rounded quantity tiers.
     */
    val app_discount: DiscountFunc = order => {
      val roundedQuantity = math.min(((order.quantity + 4) / 5) * 5, order.quantity)
      math.min((roundedQuantity / 5) * 5 / 100.0, 0.2)
    }


    /**
     * Condition 6: Payment method is Visa.
     */
    val is_visa: BoolFunc = order => order.payment_method == "Visa"

    /**
     * Fixed 5% discount for Visa payment method.
     */
    val visa_discount: DiscountFunc = _ => 0.05

    /**
     * Returns a list of tuples pairing qualifying conditions with discount functions.
     * @return List of (BoolFunc, DiscountFunc) tuples representing discount rules.
     */
    def get_discountFunctions(): List[(BoolFunc, DiscountFunc)] = List(
      (is_less30, less30_Discount),
      (is_chess_wine, chee_wine_Discount),
      (is_sold23March, sold23March_Discount),
      (is_more5, more5_Discount),
      (is_app, app_discount),
      (is_visa, visa_discount)
    )

    val discountRules = get_discountFunctions()

    // -------------------------- Processing orders with discounts --------------------------

    /**
     * Processes raw CSV lines into tuples of:
     * (product_name, total_before_discount, average_discount_percent, total_after_discount)
     * - Calculates total price before discount.
     * - Collects applicable discounts for each order.
     * - Averages top two discounts.
     * - Applies average discount to compute final price.
     */
    val processedOrders = lines.map(to_order).map { order =>
      val totalBefore = f"${order.unit_price * order.quantity}%.2f".toDouble

      val matchingDiscounts = discountRules.collect {
        case (cond, disc) if cond(order) => disc(order)
      }

      val topTwo = matchingDiscounts.sorted(Ordering[Double].reverse).take(2)
      val avgDiscount = if (topTwo.nonEmpty) topTwo.sum / topTwo.size else 0.0

      val totalAfter = f"${totalBefore - (totalBefore * avgDiscount)}%.2f".toDouble

      (order, totalBefore, avgDiscount, totalAfter)
//      (order.product_name, totalBefore,
//        f"${avgDiscount * 100}%.2f".toDouble, totalAfter)
    }

    // -------------------------- Load processed data into database --------------------------

    /**
     * SQL insert statement for storing processed orders.
     */
    def fixTimestamp(ts: String) = {
      ZonedDateTime.parse(ts).toLocalDateTime.toString.replace("T", " ")
    }

    val insertSql = "INSERT INTO order_result (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, total_before, discount, total_after) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      Using.resource(db_connection.connection) { conn =>
        Using.resource(conn.prepareStatement(insertSql)) { preparedStatement =>
          processedOrders.foreach { case (order, totalBefore, avgDiscount, totalAfter) =>
            preparedStatement.setString(1, fixTimestamp(order.timestamp))
            preparedStatement.setString(2, order.product_name)
            preparedStatement.setString(3, order.expiry_date)
            preparedStatement.setInt(4, order.quantity)
            preparedStatement.setDouble(5, order.unit_price)
            preparedStatement.setString(6, order.channel)
            preparedStatement.setString(7, order.payment_method)
            preparedStatement.setDouble(8, totalBefore)
            preparedStatement.setDouble(9, avgDiscount)
            preparedStatement.setDouble(10, totalAfter)

            preparedStatement.addBatch()
          }
          preparedStatement.executeBatch()
        }
      }


    } catch {
      case e: Exception =>
        logger.severe(s"Error inserting processed orders into database: ${e.getMessage}")
        throw e
    }

    logger.info("Order processing pipeline completed successfully.")

  } catch {
    case e: Exception =>
      logger.severe(s"Fatal error in order processing pipeline: ${e.getMessage}")
      throw e
  }
}