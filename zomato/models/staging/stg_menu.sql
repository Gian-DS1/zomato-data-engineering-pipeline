select  -- Limpia la tabla de menú para que cada fila represente una relación utilizable entre restaurante, alimento y precio
		menu_id,  -- Conserva el identificador del menú porque identifica de forma única cada registro del catálogo
		try_to_number(r_id) as restaurant_id,  -- Convierte r_id a número para que coincida con la llave del restaurante en otros modelos
		f_id,  -- Conserva el id del alimento para enlazar el menú con el catálogo de food
		cuisine,  -- Conserva el tipo de cocina porque ayuda a entender qué oferta tiene cada restaurante
		try_to_decimal(price, 10, 2) as price  -- Convierte el precio a decimal con dos decimales para hacer cálculos confiables
from {{ source('raw', 'menu') }}  -- Lee la tabla raw de menu donde viven los registros originales
where try_to_number(r_id) is not null  -- Excluye filas sin restaurante válido porque no se pueden relacionar correctamente
	and try_to_decimal(price, 10, 2) > 0  -- Excluye precios vacíos o cero para quedarte solo con valores útiles de negocio
