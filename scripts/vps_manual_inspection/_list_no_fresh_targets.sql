-- Events from the first week-run discovery window with zero ticket_types rows.
PRAGMA busy_timeout = 8000;

SELECT e.event_url AS target_url, e.event_slug
FROM events e
LEFT JOIN ticket_types tt ON tt.event_id = e.event_id
WHERE e.first_seen_at_utc >= '2026-05-03T22:54:00Z'
  AND e.first_seen_at_utc <= '2026-05-03T23:17:00Z'
GROUP BY e.event_id
HAVING COUNT(tt.ticket_type_id) = 0
ORDER BY e.event_url;
