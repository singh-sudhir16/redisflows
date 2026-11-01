-- Record a worker heartbeat: liveness zset (score = last seen) plus an info hash with a TTL.
--
-- KEYS[1] workers zset   KEYS[2] worker info hash
-- ARGV[1] worker id      ARGV[2] info TTL ms   ARGV[3..] info field/value pairs
--
-- Returns the server time of the heartbeat.

local now = now_ms()
redis.call('ZADD', KEYS[1], int(now), ARGV[1])

local fields = {'last_seen', int(now)}
for i = 3, #ARGV do fields[#fields + 1] = ARGV[i] end
redis.call('HSET', KEYS[2], unpack(fields))
redis.call('PEXPIRE', KEYS[2], ARGV[2])

-- prune workers that have been silent for a long time
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', int(now - 10 * tonumber(ARGV[2])))
return int(now)
