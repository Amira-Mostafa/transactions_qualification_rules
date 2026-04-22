package rules

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.io.{Codec, Source}
import scala.util.{Try, Using}
import rules.logger.logger
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import rules.Order

/**
 * Object `qualification_rules` is the main entry point of the order processing pipeline.
 *
 * Responsibility:
 *   Reads raw order rows from a CSV file, applies discount rules to each order,
 *   calculates final prices, and bulk-inserts results into a MySQL database —
 *   using parallel processing and batch DB writes to finish in upmost ~6 minutes
 * - Logs key pipeline stages and handles errors gracefully.
 *
 * The pipeline steps:
 * 1. Stream the CSV file in chunks (never load all rows into memory at once).
 * 2. Define various discount rules, each with:
 *    - A Boolean predicate function to check if an order qualifies, if true it qualifies if false it's not.
 *    - A discount function returning the discount amount.
 * 4. Parse each line to an Order object.
 * 5. For each order:
 *    - Calculate total price before discount.
 *    - Collect all discounts that apply to the order.
 *    - Pick the top two highest discounts and average them.
 *    - Calculate the final total price after applying the average discount.
 * 6. Bulk-insert the processed chunk into MySQL with a single batched statement.
 * 7. Log progress after every chunk so you can track throughput in real time.
 *
 */
object qualification_rules extends App {

  val startTime = System.currentTimeMillis()

  // ─────────────────────────────────────────────────────────────────────────
  // PIPELINE CONFIGURATION
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Number of CSV lines processed in each batch.
   * 80,000 was chosen after testing:
   */
  val BATCH_SIZE = 80000

  /**
   * Number of logical CPU cores available on this machine, it will use 4 threads
   * on a quad-core laptop and 16 on a server without any code change.
   */
  val PARALLELISM = Runtime.getRuntime.availableProcessors()

  /** Path to the input file. */
  val FILE_PATH = "src/main/resources/TRX1000.csv"


  logger.info(s"Starting parallel processing pipeline...")
  logger.info(s"Configuration: Batch Size=$BATCH_SIZE, Parallelism=$PARALLELISM")

  // ─────────────────────────────────────────────────────────────────────────
  // THREAD POOL
  // ─────────────────────────────────────────────────────────────────────────

  /**
   *  ForkJoinPool was used for Scala parallelism.
   *
   * Why PARALLELISM / 2 threads?
   * Using all cores for computation for better performance
   * Half the cores for computation + half for everything else to avoid crashing.
   * The pool is shut down in the `finally` block to release OS threads cleanly.
   */
  val forkJoinPool = new java.util.concurrent.ForkJoinPool(PARALLELISM / 2)

  try {

    // ───────────────────────────────────────────────────────────────────────
    // HELPER FUNCTIONS
    // ───────────────────────────────────────────────────────────────────────

    /**
     * Parses one CSV line into an [[Order]].
     * @param line A raw CSV line string with exactly 7 comma-separated fields.
     * @return A fully constructed [[Order]] instance.
     *
     */
    def to_order(line: String): Order = {
      val parts = line.split(",")
      if (parts.length != 7)
        throw new IllegalArgumentException(s"Invalid line format: $line")
      Order(parts(0), parts(1), parts(2), parts(3).toInt,
        parts(4).toDouble, parts(5), parts(6))
    }

    /**
     * Extracts the date portion (yyyy-MM-dd) from an ISO 8601 timestamp.
     * e.g. "2023-04-18T18:18:40Z" → "2023-04-18"
     * Splitting on 'T' and taking index 0 which is the date
     */
    def process_date(d: String): String = d.split('T')(0)

    /**
     * Extracts the day-of-month integer from a yyyy-MM-dd date string.
     * e.g. "2023-03-23" → 23
     */
    def extract_day(d: String): Int = d.split('-')(2).toInt

    /**
     * Extracts the month integer from a yyyy-MM-dd date string.
     * e.g. "2023-03-23" → 3
     */
    def extract_month(d: String): Int = d.split('-')(1).toInt

    /**
     * Parses a yyyy-MM-dd string into a localtime
     * Used by date-arithmetic rules that need to compute day differences.
     */
    def toDate(d: String): LocalDate = LocalDate.parse(d)

    /**
     * Computes the number of days between two dates
     */
    def calc_days(startDate: LocalDate, endDate: LocalDate): Long =
      ChronoUnit.DAYS.between(startDate, endDate)

    /**
     * Converts an ISO 8601 timestamp to a MySQL-compatible DATETIME string.
     *
     * MySQL's DATETIME type does not accept the ISO 'T' separator or the trailing 'Z'.
     * e.g. "2023-04-18T18:18:40Z" → "2023-04-18 18:18:40"
     * we only want (date + time, excluding the timezone)
     */
    def fixTimestamp(ts: String): String =
      ts.substring(0, 19).replace("T", " ")

    // ───────────────────────────────────────────────────────────────────────
    // DISCOUNT RULE TYPES
    // Each rule is represented as a pair: (qualifying condition, discount function)
    //   type BoolFunc     = the "true/false to make sure it qualifies" function
    //   type DiscountFunc = the "discount calculation after true" function
    // ───────────────────────────────────────────────────────────────────────

    /** A function that takes an Order and returns true if a discount rule applies. */
    type BoolFunc = Order => Boolean

    /** A function that takes an Order and returns the discount as a double. */
    type DiscountFunc = Order => Double

    // ───────────────────────────────────────────────────────────────────────
    // RULE 1 - expiry date
    // Qualifying: fewer than 30 days remain between order date and expiry date
    // Calculation: 1% per day closer to expiry (29 days left = 1%, 1 day left = 29%)
    // ───────────────────────────────────────────────────────────────────────

    /** Qualifies if 0 < days_until_expiry < 30 at the time of the transaction and 0 is for exact expiry date. */
    val is_less30: BoolFunc = order => {
      val orderDate  = toDate(process_date(order.timestamp))
      val expiryDate = toDate(order.expiry_date)
      val days       = calc_days(orderDate, expiryDate)
      days > 0 && days < 30
    }

    /**
     * Discount calculation for this rule
     */
    val less30_Discount: DiscountFunc = order => {
      val orderDate  = toDate(process_date(order.timestamp))
      val expiryDate = toDate(order.expiry_date)
      val rem_days   = calc_days(orderDate, expiryDate)
      (30 - rem_days) / 100.0
    }

    // ───────────────────────────────────────────────────────────────────────
    // RULE 2 — Cheese / Wine product discount
    // Qualifying: product name contains "cheese" or "wine" (case-insensitive)
    // Calculation: cheese → 10%, wine → 5%
    // ───────────────────────────────────────────────────────────────────────

    /**
     * checks on lowercased product name to handle any combinatio of category prefix and product name
     */
    val is_chess_wine: BoolFunc = order => {
      val product = order.product_name.toLowerCase
      product.contains("cheese") || product.contains("wine")
    }

    /** Returns 0.10 for cheese, 0.05 for wine, 0.0 as non applicable. */
    val chee_wine_Discount: DiscountFunc = order => {
      val product = order.product_name.toLowerCase
      if      (product.contains("cheese")) 0.10
      else if (product.contains("wine"))   0.05
      else                                 0.0
    }

    // ───────────────────────────────────────────────────────────────────────
    // RULE 3 — March 23rd special discount
    // Qualifying: transaction date is exactly March 23rd (any year)
    // Calculation: 50% discount
    // ───────────────────────────────────────────────────────────────────────

    /** Checks day == 23 and month == 3 from the order's timestamp date portion. */
    val is_sold23March: BoolFunc = order => {
      val dateStr = process_date(order.timestamp)
      extract_day(dateStr) == 23 && extract_month(dateStr) == 3
    }

    /**
     * Fixed 50% discount
     */
    val sold23March_Discount: DiscountFunc = _ => 0.5

    // ───────────────────────────────────────────────────────────────────────
    // RULE 4 — Bulk quantity discount
    // Qualifying: quantity > 5
    // Calculation: 6-9 → 5%, 10-14 → 7%, 15+ → 10%
    // ───────────────────────────────────────────────────────────────────────

    /** Qualifies if the customer bought more than 5 units of the same product. */
    val is_more5: BoolFunc = order => order.quantity > 5

    /** Returns a layered discount based on how many units were purchased. */
    val more5_Discount: DiscountFunc = order => {
      val q = order.quantity
      if      (q >= 6  && q <= 9)  0.05
      else if (q >= 10 && q <= 14) 0.07
      else if (q >= 15)            0.10
      else                         0.0
    }

    // ───────────────────────────────────────────────────────────────────────
    // RULE 5 — App channel discount
    // Qualifying: order placed through the "App" channel
    // Calculation: quantity-based tiered discount, capped at 20%
    // ───────────────────────────────────────────────────────────────────────

    /** Qualifies if the sales channel is exactly "App". */
    val is_app: BoolFunc = order => order.channel == "App"

    /**
     * Rounding quantity up to the nearest multiple of 5, then divides by 100.
     * Result is capped at 0.20 (20%) to prevent runaway discounts on very large orders.
     * e.g. quantity=7 → rounded to 10 → 10/5=2 → 2*5=10 → 10/100 = 0.10 (10%)
     */
    val app_discount: DiscountFunc = order => {
      val roundedQuantity = math.min(((order.quantity + 4) / 5) * 5, order.quantity)
      math.min((roundedQuantity / 5) * 5 / 100.0, 0.2)
    }

    // ───────────────────────────────────────────────────────────────────────
    // RULE 6 — Visa payment discount
    // Qualifying: payment method is "Visa"
    // Calculation: flat 5% discount
    // ───────────────────────────────────────────────────────────────────────

    /** Qualifies if the customer paid with Visa. */
    val is_visa: BoolFunc = order => order.payment_method == "Visa"

    /** 5% discount for all Visa transactions. */
    val visa_discount: DiscountFunc = _ => 0.05

    // ───────────────────────────────────────────────────────────────────────
    // RULES Structure
    // A List of (BoolFunc, DiscountFunc) pairs
    // To add a new rule, we can append a new tuple.
    // ───────────────────────────────────────────────────────────────────────

    val discountRules: List[(BoolFunc, DiscountFunc)] = List(
      (is_less30,       less30_Discount),
      (is_chess_wine,   chee_wine_Discount),
      (is_sold23March,  sold23March_Discount),
      (is_more5,        more5_Discount),
      (is_app,          app_discount),
      (is_visa,         visa_discount)
    )

    // ───────────────────────────────────────────────────────────────────────
    // ORDER PROCESSOR
    // ───────────────────────────────────────────────────────────────────────

    /**
     *
     * Applies all discount rules to a single order and computes the final price.
     *
     * @param order The order to process.
     * @return A 4-tuple: (order, totalBefore, avgDiscountFraction, totalAfter)
     *
     */
    def processOrder(order: Order): (Order, Double, Double, Double) = {
      val totalBefore = order.unit_price * order.quantity

      // collect = filter + map
      // runs the discount function when the qualifier returns true
      val matchingDiscounts: List[Double] = discountRules.collect {
        case (cond, disc) if cond(order) => disc(order)
      }

      // Sort descending, take top 2, average — or 0.0 if nothing qualified
      val topTwo      = matchingDiscounts.sorted(Ordering[Double].reverse).take(2)
      val avgDiscount = if (topTwo.nonEmpty) topTwo.sum / topTwo.size else 0.0

      val totalAfter = totalBefore - (totalBefore * avgDiscount)

      (order, totalBefore, avgDiscount, totalAfter)
    }

    logger.info("Starting to read and process file...")

    // ───────────────────────────────────────────────────────────────────────
    // DATABASE INSERT STATEMENT
    // Prepared once, reused for all the batches to avoid the same parse and execution plan every time it is called
    // ───────────────────────────────────────────────────────────────────────

    val insertSql =
      """INSERT INTO order_result
        |(timestamp, product_name, expiry_date, quantity, unit_price,
        | channel, payment_method, total_before, discount, total_after)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin


    Using.resource(db_connection.connection) { conn =>

      // Disable auto-commit so we can commit manually after each batch.
      // This will group 80K inserts into one DB transaction per batch
      conn.setAutoCommit(false)

      Using.resource(conn.prepareStatement(insertSql)) { preparedStatement =>

        // Mutable counters for logging perposes only
        var totalProcessed = 0
        var batchNumber    = 0

        // Only one line exists in memory at a time before it enters a batch.
        Using.resource(Source.fromFile(FILE_PATH)(Codec.UTF8)) { source =>
          val lines = source.getLines()

          // Skip the header
          if (lines.hasNext) lines.next()

          // pull only 80K lines at a time ->  process them -> insert to DB
          // for example -> the main thread will wait for batch 1 to compleletly FINISH before reading batch 2 -> the one batch has more than one core to process it
          lines.grouped(BATCH_SIZE).foreach { batch =>
            batchNumber += 1
            val batchStartTime = System.currentTimeMillis()

            // ── PARALLEL PROCESSING ──────────────────────────────────────
            // Split ONE batch across ALL cores Each core parses and processes its chuck of lines and all cores work on the same batch.
            val parBatch = batch.par
            parBatch.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

            val processedBatch: Seq[(Order, Double, Double, Double)] =
              parBatch.flatMap { line =>
                try   Some(processOrder(to_order(line)))
                catch {
                  case e: Exception =>
                    logger.warning(s"Skipping line: ${e.getMessage}")
                    None
                }
              }.seq // converts ParSeq → Seq so we can hand it to JDBC cause db need data sequentially

            // ── BATCH DB INSERT ──────────────────────────────────────────
            // Set parameters for each row, then addBatch()
            processedBatch.foreach { case (order, totalBefore, avgDiscount, totalAfter) =>
              preparedStatement.setString(1, fixTimestamp(order.timestamp))
              preparedStatement.setString(2, order.product_name)
              preparedStatement.setString(3, order.expiry_date)
              preparedStatement.setInt   (4, order.quantity)
              preparedStatement.setDouble(5, order.unit_price)
              preparedStatement.setString(6, order.channel)
              preparedStatement.setString(7, order.payment_method)
              preparedStatement.setDouble(8, totalBefore)
              preparedStatement.setDouble(9, avgDiscount)
              preparedStatement.setDouble(10, totalAfter)
              preparedStatement.addBatch()
            }

            // executeBatch() sends ALL rows in a single network packet.
            // With rewriteBatchedStatements=true in the JDBC URL, MySQL further
            // rewrites this into one combined INSERT ... VALUES (...),(...),...
            preparedStatement.executeBatch()

            // Commit the batch as one DB transaction -> then move to the next batch.
            conn.commit()

            totalProcessed += processedBatch.size
            val batchTime = (System.currentTimeMillis() - batchStartTime) / 1000.0

            logger.info(
              s"Batch $batchNumber: Processed ${processedBatch.size} orders in ${batchTime}s " +
                s"(Total: $totalProcessed, Rate: ${(processedBatch.size / batchTime).toInt} orders/sec)"
            )
          }
        }

        // ── FINAL SUMMARY for logging ────────────────────────────────────────────────
        val duration = (System.currentTimeMillis() - startTime) / 1000.0

        logger.info("==========================================")
        logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
        logger.info(s"Total orders processed : $totalProcessed")
        logger.info(s"Total time             : $duration seconds (${duration / 60} minutes)")
        logger.info(s"Average rate           : ${(totalProcessed / duration).toInt} orders/second")
        logger.info("==========================================")
      }
    }

  } catch {
    case e: Exception =>
      logger.severe(s"Fatal error in pipeline: ${e.getMessage}")
      e.printStackTrace()
      throw e

  } finally {
    // regardless of success or failure shut down.
    forkJoinPool.shutdown()
  }
}
