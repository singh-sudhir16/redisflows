-- List members of a time-scored zset whose time has come (score <= server now).
--
-- KEYS[1] zset   ARGV[1] max members

return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', int(now_ms()), 'LIMIT', 0, ARGV[1])
