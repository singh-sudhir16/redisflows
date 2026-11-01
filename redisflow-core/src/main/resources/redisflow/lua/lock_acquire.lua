-- Acquire a lock with a TTL and hand out a monotonically increasing fencing token.
-- Downstream systems can reject writes carrying a token older than the last one seen,
-- which protects against a paused holder whose lock already expired.
--
-- KEYS[1] lock key   KEYS[2] fencing counter
-- ARGV[1] holder     ARGV[2] ttl ms
--
-- Returns the fencing token (> 0), or 0 if the lock is held by someone else.

if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
