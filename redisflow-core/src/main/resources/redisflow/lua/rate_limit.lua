-- Sliding-window-log rate limiter.
--
-- KEYS[1] window zset (one member per accepted request, score = time)
-- ARGV[1] window ms   ARGV[2] limit   ARGV[3] unique request id
--
-- Returns {allowed (1|0), remaining, retry_after_ms}

local now = now_ms()
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', int(now - window))
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
  redis.call('ZADD', KEYS[1], int(now), ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, limit - count - 1, 0}
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry_after = window
if oldest[2] then retry_after = tonumber(oldest[2]) + window - now end
if retry_after < 1 then retry_after = 1 end
return {0, 0, retry_after}
