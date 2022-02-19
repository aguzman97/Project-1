SELECT name AS "Ticker Name",
         MAX(high) AS "High Price",
         SUBSTR("ts",12,2) AS "Hour"
FROM "stock-db"."sta9760f2021_stream1"
GROUP BY  name, SUBSTR("ts", 12, 2)
ORDER BY  name, SUBSTR("ts", 12, 2)}