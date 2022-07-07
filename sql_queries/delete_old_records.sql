/*
CONTEXTO:
- Se requiere eliminar todos los registros que superen los 60 días de antigüedad

RESULTADOS ESPERADOS:
- Eliminar todos los registros con más de 60 días de antigüedad. 

*/

DELETE FROM {}.{}
WHERE DATEDIFF(dd, "FechaExtraccion", GETDATE()) > 60;