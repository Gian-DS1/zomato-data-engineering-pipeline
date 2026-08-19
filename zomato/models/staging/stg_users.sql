select  -- Normaliza la tabla de usuarios para que funcione como dimensión limpia dentro del modelo
    user_id::number as customer_id,  -- Convierte el id del usuario a número para usarlo como llave consistente en joins
    name as customer_name,  -- Renombra el campo name para dejar claro que representa al cliente
    lower(email) as email,  -- Convierte el correo a minúsculas para evitar diferencias por formato en comparaciones
    try_to_number(age) as age,  -- Convierte la edad a número para poder segmentar clientes por rango etario
    gender,  -- Conserva el género porque es un atributo descriptivo útil para análisis demográfico
    marital_status,  -- Conserva el estado civil como variable descriptiva del perfil del cliente
    occupation,  -- Conserva la ocupación porque ayuda a entender el contexto socioeconómico del usuario
    monthly_income as income_band,  -- Renombra el ingreso mensual para que se entienda mejor que es una banda o categoría de ingreso
    education,  -- Conserva el nivel educativo como dato descriptivo del perfil de cliente
    try_to_number(family_size) as family_size  -- Convierte el tamaño de familia a número para segmentaciones y cruces analíticos
from {{ source('raw', 'users') }}  -- Lee la tabla raw de users desde la fuente declarada en dbt
where try_to_number(user_id) is not null  -- Excluye filas con id inválido para evitar registros que no se puedan relacionar