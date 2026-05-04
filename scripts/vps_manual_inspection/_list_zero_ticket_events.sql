PRAGMA busy_timeout = 8000;
SELECT 'events_total', COUNT(*) FROM events;
SELECT e.event_url AS target_url
FROM events e
LEFT JOIN ticket_types tt ON tt.event_id = e.event_id
GROUP BY e.event_id
HAVING COUNT(tt.ticket_type_id) = 0
ORDER BY e.event_url;
