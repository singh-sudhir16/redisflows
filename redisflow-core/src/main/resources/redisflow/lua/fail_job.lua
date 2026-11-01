-- Record a failed attempt: schedule a retry with backoff, or dead-letter the job.
-- The decision uses the authoritative attempt counter stored in Redis.
--
-- KEYS[1] job hash   KEYS[2] ack key   KEYS[3] lease hash   KEYS[4] stats hash
-- KEYS[5] retry zset KEYS[6] dead-letter list
--
-- ARGV[1] ack kind   ARGV[2] ack ref   ARGV[3] consumer group
-- ARGV[4] error message                ARGV[5] retry delay ms
-- ARGV[6] retry member ("queue:job_id") ARGV[7] job id
-- ARGV[8] force dead ('1' = non-retryable failure)
-- ARGV[9] event channel                ARGV[10] worker id
--
-- Returns 'retrying' | 'dead' | the existing terminal status | 'missing'

ack(ARGV[1], KEYS[2], KEYS[3], ARGV[2], ARGV[3])

if redis.call('EXISTS', KEYS[1]) == 0 then return 'missing' end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' or status == 'dead' then return status end

local now = now_ms()
local info = redis.call('HMGET', KEYS[1], 'attempts', 'max_attempts', 'type', 'queue')
local attempts = tonumber(info[1]) or 0
local max_attempts = tonumber(info[2]) or 1
local event = {job_id = ARGV[7], job_type = info[3], queue = info[4], worker_id = ARGV[10],
               attempt = attempts, error = ARGV[4]}

if ARGV[8] ~= '1' and attempts < max_attempts then
  local retry_at = now + tonumber(ARGV[5])
  redis.call('HSET', KEYS[1], 'status', 'retrying', 'last_error', ARGV[4], 'worker_id', '',
             'next_retry_at', int(retry_at))
  redis.call('ZADD', KEYS[5], int(retry_at), ARGV[6])
  redis.call('HINCRBY', KEYS[4], 'retried', 1)
  event['type'] = 'job.retrying'
  event['next_retry_at'] = retry_at
  emit(ARGV[9], event, now)
  return 'retrying'
end

redis.call('HSET', KEYS[1], 'status', 'dead', 'last_error', ARGV[4], 'completed_at', int(now))
redis.call('HDEL', KEYS[1], 'next_retry_at')
redis.call('LPUSH', KEYS[6], ARGV[7])
redis.call('HINCRBY', KEYS[4], 'dead', 1)
event['type'] = 'job.dead'
emit(ARGV[9], event, now)
return 'dead'
