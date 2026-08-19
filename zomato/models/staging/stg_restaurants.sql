select  -- Selecciona y normaliza las columnas de restaurantes para convertir datos crudos en un modelo limpio
    id::number as restaurant_id,  -- Convierte el id original a número para estandarizar la llave del restaurante y facilitar joins
    name as restaurant_name,  -- Conserva el nombre del restaurante con un alias más claro para el modelo de staging
    trim(coalesce(regexp_substr(city, '[^,]+$'), city)) as city,  -- Limpia el texto de ciudad y, si viene con ciudad y estado, toma la última parte útil
    try_to_decimal(nullif(rating, '--'), 3, 1) as rating,  -- Convierte rating a decimal y reemplaza '--' para evitar errores con valores vacíos o inválidos
    try_to_number(regexp_substr(rating_count, '[0-9]+')) as rating_count,  -- Extrae solo la parte numérica del texto de reseñas para poder tratarla como cantidad
    try_to_number(regexp_substr(cost, '[0-9]+')) as cost_for_two,  -- Extrae el valor numérico del costo para dos personas y descarta símbolos o texto extra
    cuisine,  -- Conserva el tipo de cocina porque ya viene como un atributo útil para análisis
    lic_no as license_no  -- Renombra el número de licencia para que el significado del campo sea más claro en el modelo
from {{ source('raw', 'restaurants') }}  -- Lee la tabla raw de restaurants directamente desde la fuente declarada en dbt
where try_to_number(id) is not null  -- Filtra filas donde el id realmente se puede convertir a número y evita registros corruptos
    