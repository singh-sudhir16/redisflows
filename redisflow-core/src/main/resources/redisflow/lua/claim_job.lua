-- Claim a delivered job for processing.
--
-- KEYS[1] job hash
-- ARGV[1] worker id   ARGV[2] event channel
--
-- Terminal jobs (completed / dead) are returned untouched so the caller can
-- acknowledge and skip a duplicate delivery. Otherwise the job is marked
-- processing and its attempt counter is incremented in the same atomic step.
-- Returns the job hash as a flat field/value list (empty if the job is gone).

if redis.call('EXISTS', KEYS[1]) == 0 then return {} end

local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' or status == 'dead' then
  return redis.call('HGETALL', KEYS[1])
end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'processing', 'worker_id', ARGV[1], 'started_at', int(now))
local attempt = redis.call('HINCRBY', KEYS[1], 'attempts', 1)

local info = redis.call('HMGET', KEYS[1], 'id', 'type', 'queue')
emit(ARGV[2], {type = 'job.started', job_id = info[1], job_type = info[2], queue = info[3],
               worker_id = ARGV[1], attempt = attempt}, now)
return redis.call('HGETALL', KEYS[1])
