-- List members of a time-scored zset seen within the last N ms (server clock).
--
-- KEYS[1] zset   ARGV[1] window ms

return redis.call('ZRANGEBYSCORE', KEYS[1], int(now_ms() - tonumber(ARGV[1])), '+inf')
