-- SQLite
-- shipments that were late or missing delivery confirmation

SELECT 
    shipment_id,
    carrier_name, 
    pickup_city,
    delivery_city,
    scheduled_delivery,
    actual_delivery,
    CASE
        WHEN actual_delivery IS NULL THEN 'Missing Confirmation'
        WHEN actual_delivery > scheduled_delivery THEN 'Late'
        ELSE 'On Time'
    END AS delivery_status
FROM raw_shipments
WHERE
    date(scheduled_delivery) < date('now')
    AND (actual_delivery IS NULL OR date(actual_delivery) > date(scheduled_delivery))
ORDER BY delivery_status, carrier_name;


-- worst performed carriers in the last month

SELECT 
    carrier_name, 
    COUNT(*) AS total_shipments,
    SUM(CASE
        WHEN actual_delivery > scheduled_delivery THEN 1 ELSE 0 END) AS late_shipments,
        ROUND(
             100.0 * SUM(CASE WHEN actual_delivery > scheduled_delivery THEN 1 ELSE 0 END) / COUNT(*),
        1
    ) AS late_percentage
FROM raw_shipments
GROUP BY carrier_name
HAVING COUNT(*) >= 2
ORDER BY late_percentage DESC;

-- Official SCAC code and insurance status for each shipment 
SELECT 
    s.shipment_id,
    s.carrier_name,
    c.scac_code,
    c.insurance_status,
    s.scheduled_delivery,
    s.actual_delivery
FROM raw_shipments s
LEFT JOIN carrier_directory c 
    ON LOWER(s.carrier_name) LIKE '%' || LOWER(c.carrier_legal_name) || '%'
ORDER BY s.shipment_id;
