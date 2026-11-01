-- Extend a lock's TTL only if the caller still holds it (compare-and-expire).
--
-- KEYS[1] lock key   ARGV[1] holder   ARGV[2] ttl ms
-- Returns 1 if renewed, 0 otherwise.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
