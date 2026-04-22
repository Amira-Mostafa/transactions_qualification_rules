package rules

case class Order (
  timestamp: String,
  product_name: String,
  expiry_date: String,
  quantity: Int,
  unit_price: Double,
  channel: String,
  payment_method: String
                 )

