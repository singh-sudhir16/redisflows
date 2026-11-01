-- Return expired priority leases to the priority set with their original score,
-- so a job leased by a crashed worker keeps its place in line.
--
-- KEYS[1] priority zset   KEYS[2] in-flight zset   KEYS[3] lease hash
-- ARGV[1] max leases to reap
--
-- Returns the number of leases re-queued.

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', int(now_ms()), 'LIMIT', 0, ARGV[1])
for _, member in ipairs(expired) do
  local score = redis.call('HGET', KEYS[3], member) or priority_score(3, now_ms())
  redis.call('ZADD', KEYS[1], score, member)
  redis.call('ZREM', KEYS[2], member)
  redis.call('HDEL', KEYS[3], member)
end
return #expired
