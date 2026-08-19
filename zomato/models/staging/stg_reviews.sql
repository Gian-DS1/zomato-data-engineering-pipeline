select  -- Construye una versión limpia de reviews para relacionarla con otros modelos y analizar opiniones
	r.review_id,  -- Conserva el identificador de la reseña para poder rastrear cada opinión individual
	r.order_id,  -- Conserva el id de la orden para conectar la reseña con la compra original
	r.user_id::number as customer_id,  -- Convierte el usuario a número porque en staging queremos llaves homogéneas
	r.restaurant_id::number as restaurant_id,  -- Convierte el restaurante a número para que coincida con el modelo de restaurantes
	r.rating::number as rating,  -- Convierte la calificación a número para poder calcular promedios y métricas
	r.comment::string as comment,  -- Fuerza el comentario a texto porque es un campo descriptivo libre
	r.review_date::date as review_date,  -- Normaliza la fecha para que se pueda filtrar y agrupar fácilmente por tiempo
	res.city as city  -- Trae la ciudad del restaurante desde stg_restaurants para enriquecer la reseña con contexto geográfico
from {{ source('raw', 'reviews') }} r  -- Lee la tabla raw de reviews y usa r como alias para que la consulta sea más compacta
left join {{ ref('stg_restaurants') }} res  -- Cruza con el staging de restaurantes para agregar atributos ya limpios
	on r.restaurant_id = res.restaurant_id  -- Une ambas tablas por el id del restaurante para obtener la ciudad correcta
where r.comment is not null  -- Excluye reseñas sin comentario porque no aportan texto para análisis cualitativo