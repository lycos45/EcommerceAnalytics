package com.ecommerce.models

// Les champs numeriques sont en Option : le fichier contient des valeurs manquantes
case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String,          // yyyyMMddHHmmss
  location: String,
  payment_method: String,
  category: String
)
