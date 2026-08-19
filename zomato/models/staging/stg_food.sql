SELECT
    -- Limpia el catálogo de alimentos para dejarlo listo para análisis y joins
    f_id,
    -- Conserva el identificador del alimento porque es la clave principal del catálogo
    item AS food_name,
    -- Renombra item a food_name para que el significado del campo sea más obvio
    INITCAP(veg_or_non_veg) AS veg_or_non_veg -- Normaliza el texto para que el valor visual quede consistente entre registros
FROM
    {{ source(
        'raw',
        'food'
    ) }}
    -- Lee los datos crudos del catálogo de alimentos desde la fuente raw
WHERE
    f_id IS NOT NULL -- Descarta filas sin identificador porque un catálogo sin clave no se puede relacionar bien
