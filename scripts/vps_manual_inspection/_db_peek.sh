#!/usr/bin/env bash
cd /root/festival-ticket-model
sqlite3 ticketswap.db "PRAGMA busy_timeout=8000; select 'events_total', count(*) from events;"
sqlite3 ticketswap.db "select 'max_first_seen', max(first_seen_at_utc) from events;"
sqlite3 ticketswap.db "select 'ticket_types_total', count(*) from ticket_types;"
sqlite3 ticketswap.db "select 'max_tt_first_seen', max(first_seen_at_utc) from ticket_types;"
sqlite3 ticketswap.db "select event_id, first_seen_at_utc from events order by first_seen_at_utc desc limit 5;"
