select  -- Limpia los detalles de cada línea de pedido para analizar productos vendidos por orden
	order_item_id,  -- Conserva el identificador único de cada renglón del pedido
	order_id,  -- Conserva la orden padre para poder agrupar todos los items de una compra
	r_id as restaurant_id,  -- Renombra el restaurante para alinearlo con el resto de modelos
	f_id,  -- Conserva el alimento vendido para cruzarlo con el catálogo de food
	price::decimal(10, 2) as price,  -- Convierte el precio unitario a decimal porque así se pueden sumar y comparar mejor
	quantity::number as quantity,  -- Convierte la cantidad a número para poder calcular totales con precisión
	line_amount::decimal(10, 2) as line_amount,  -- Convierte el total de la línea a decimal para análisis monetario confiable
from {{ source('raw', 'order_items') }}  -- Lee la tabla raw de order_items con los datos originales del detalle