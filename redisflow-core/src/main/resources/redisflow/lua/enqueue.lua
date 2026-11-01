-- Atomically create a job and make it visible to workers.
--
-- KEYS[1] job hash          KEYS[2] destination (stream | priority zset | scheduled zset)
-- KEYS[3] known-queues set  KEYS[4] stats hash   KEYS[5] idempotency key
--
-- ARGV[1] mode: 'stream' | 'priority' | 'scheduled'
-- ARGV[2] queue             ARGV[3] job id        ARGV[4] priority
-- ARGV[5] scheduled_for (epoch ms, scheduled mode only)
-- ARGV[6] idempotency TTL ms ('0' = no idempotency key)
-- ARGV[7] request fingerprint
-- ARGV[8] event channel ('' disables events)
-- ARGV[9] job type
-- ARGV[10..] remaining hash field/value pairs
--
-- Returns {'created', created_at} | {'replayed', job_id} | {'conflict', job_id}

if tonumber(ARGV[6]) > 0 then
  local existing = redis.call('GET', KEYS[5])
  if existing then
    local sep = string.find(existing, '|', 1, true)
    local existing_id = string.sub(existing, 1, sep - 1)
    if string.sub(existing, sep + 1) ~= ARGV[7] then
      return {'conflict', existing_id}
    end
    return {'replayed', existing_id}
  end
  redis.call('SET', KEYS[5], ARGV[3] .. '|' .. ARGV[7], 'PX', ARGV[6])
end

local now = now_ms()
local status = 'pending'
if ARGV[1] == 'scheduled' then status = 'scheduled' end

local fields = {'id', ARGV[3], 'queue', ARGV[2], 'type', ARGV[9], 'priority', ARGV[4],
                'status', status, 'attempts', '0', 'created_at', int(now)}
for i = 10, #ARGV do fields[#fields + 1] = ARGV[i] end
redis.call('HSET', KEYS[1], unpack(fields))
redis.call('SADD', KEYS[3], ARGV[2])
redis.call('HINCRBY', KEYS[4], 'enqueued', 1)

local member = ARGV[2] .. ':' .. ARGV[3]
if ARGV[1] == 'stream' then
  redis.call('XADD', KEYS[2], '*', 'job_id', ARGV[3])
elseif ARGV[1] == 'priority' then
  redis.call('ZADD', KEYS[2], priority_score(tonumber(ARGV[4]), now), member)
else
  redis.call('ZADD', KEYS[2], ARGV[5], member)
end

emit(ARGV[8], {type = 'job.enqueued', job_id = ARGV[3], job_type = ARGV[9], queue = ARGV[2]}, now)
return {'created', int(now)}
