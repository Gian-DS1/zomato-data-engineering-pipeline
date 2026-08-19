select  -- Construye una versión limpia de órdenes para análisis de ventas, tiempos y comportamiento del cliente
    order_id,  -- Conserva el identificador de la orden como llave principal del hecho
    order_timestamp,  -- Conserva el momento exacto de la orden para análisis por hora o minuto
    order_date,  -- Conserva la fecha de la orden para agregaciones diarias o mensuales
    user_id as customer_id,  -- Renombra el usuario a customer_id para que quede consistente con el resto del modelo
    r_id as restaurant_id,  -- Renombra el restaurante como restaurant_id para poder relacionarlo con otras tablas
    trim(coalesce(regexp_substr(restaurant_city, '[^,]+$'), restaurant_city)) as city,  -- Limpia la ciudad porque a veces viene con texto adicional y solo interesa la parte final útil
    cuisine,  -- Conserva la cocina porque permite segmentar ventas por tipo de comida
    items_count,  -- Conserva el número de items para medir tamaño del pedido
    sales_qty,  -- Conserva la cantidad vendida para análisis de volumen
    subtotal,  -- Conserva el subtotal antes de descuentos e impuestos
    discount,  -- Conserva el descuento aplicado para analizar promociones
    delivery_fee,  -- Conserva la tarifa de entrega para ver el costo de servicio
    gst,  -- Conserva el impuesto GST para reconstruir el monto final de la orden
    sales_amount,  -- Conserva el total pagado por el cliente como métrica de negocio principal
    currency,  -- Conserva la moneda para saber en qué divisa se registró la transacción
    payment_method,  -- Conserva el método de pago para estudiar comportamiento de compra
    order_status,  -- Conserva el estado para distinguir pedidos completados, cancelados u otros casos
    (order_status = 'Delivered') as is_delivered,  -- Genera un indicador booleano para simplificar filtros de pedidos entregados
    customer_rating,  -- Conserva la calificación del cliente para medir satisfacción
    delivery_time_min  -- Conserva el tiempo de entrega para evaluar desempeño operativo
from {{ source('raw', 'orders') }}  -- Lee la tabla raw de orders donde está la información original del pedido