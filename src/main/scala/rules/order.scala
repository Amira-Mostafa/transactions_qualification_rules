package rules
/**
 * Case class `Order`  immutable data model of the pipeline.
 * It represents exactly one row from the input CSV file after parsing.
 *
 * Field types are kept deliberately simple (String, Int, Double) because:
 * - Dates are stored as Strings and parsed on demand inside each rule function.
 *   Parsing once per rule avoids wasted work when a rule doesn't qualify.
 * -  `to_order` can do an Order from a split CSV line with minimal error surface.
 *
 */
case class Order (
  timestamp: String,
  product_name: String,
  expiry_date: String,
  quantity: Int,
  unit_price: Double,
  channel: String,
  payment_method: String
                 )

