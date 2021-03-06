/*
CONTEXTO:
- Se requiere eliminar los registros más recientes para que estos se actualicen mediante el web service del mrSAT

RESULTADOS ESPERADOS:
- Eliminar todos los registros con máximo 2 días de antigüedad (hoy, ayer y anteayer). 

*/

DELETE FROM {}.{}
WHERE DATEDIFF(dd, "FechaExtraccion", GETDATE()) <= 4;

