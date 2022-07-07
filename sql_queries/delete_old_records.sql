/*
CONTEXTO:
- Se requiere eliminar todos los registros que superen los 60 días de antigüedad

RESULTADOS ESPERADOS:
- Eliminar todos los registros con más de 60 días de antigüedad. 

*/

DELETE FROM {}.{}
WHERE DATE_TRUNC('DAY', "FechaExtraccion") < DATE_TRUNC('DAY', now() - INTERVAL '62 days');