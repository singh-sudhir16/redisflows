-- Shared helpers, prepended to every RedisFlow script at load time.
--
-- All timestamps come from the Redis server clock (TIME) rather than the caller,
-- so API servers and workers on different hosts never disagree about "now".

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Format a number as an integer string without scientific notation.
-- Lua 5.1's default number formatting loses precision above 14 digits.
local function int(n)
  return string.format('%.0f', n)
end

-- Priority score: lower priority number first, then FIFO by enqueue time.
-- priority * 1e13 + epoch_ms stays below 2^53, so the score is exact.
local function priority_score(priority, now)
  return int(priority * 1e13 + now)
end

-- Publish a lifecycle event on the given channel ('' disables events).
local function emit(channel, event, now)
  if channel == nil or channel == '' then return end
  event['timestamp'] = now
  redis.call('PUBLISH', channel, cjson.encode(event))
end

-- Acknowledge a delivery so it is not redelivered.
--   kind 'stream': ack_key = stream key, ref = message id (XACK)
--   kind 'lease' : ack_key = priority in-flight zset, lease_key = lease hash, ref = member
local function ack(kind, ack_key, lease_key, ref, group)
  if kind == 'stream' then
    redis.call('XACK', ack_key, group, ref)
  elseif kind == 'lease' then
    redis.call('ZREM', ack_key, ref)
    redis.call('HDEL', lease_key, ref)
  end
end

-- Route a runnable job to the priority set (high priority) or its stream.
local function route(job_key, stream_key, priority_key, member, job_id, threshold, now)
  local p = tonumber(redis.call('HGET', job_key, 'priority')) or 3
  if p <= tonumber(threshold) then
    redis.call('ZADD', priority_key, priority_score(p, now), member)
  else
    redis.call('XADD', stream_key, '*', 'job_id', job_id)
  end
end
