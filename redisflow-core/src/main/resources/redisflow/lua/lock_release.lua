-- Release a lock only if the caller still holds it (compare-and-delete).
--
-- KEYS[1] lock key   ARGV[1] holder
-- Returns 1 if released, 0 otherwise.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
