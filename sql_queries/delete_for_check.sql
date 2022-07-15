/*
CONTEXTO:
- Se requiere eliminar los registros más recientes para que estos se actualicen mediante el web service del mrSAT.

RESULTADOS ESPERADOS:
- Eliminar todos los registros con máximo 4 días de antigüedad EN BASE A LA MÁXIMA FECHA DE EXTRACCIÓN. 

*/
BEGIN
DELETE FROM {0}.{1}
WHERE "FechaExtraccion" >= DATEADD(DAY, -3, (SELECT MAX("FechaExtraccion") FROM {0}.{1}));
END