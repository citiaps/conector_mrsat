/*
CONTEXTO:
- Se requiere eliminar todos los registros que superen los 60 días de antigüedad

RESULTADOS ESPERADOS:
- Eliminar todos los registros con más de 60 días de antigüedad. 

*/

DELETE FROM {}.{}
WHERE CAST(NOW() AS DATE) - CAST("FechaExtraccion" AS DATE) > 62;