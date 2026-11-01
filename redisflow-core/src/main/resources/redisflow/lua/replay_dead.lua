-- Replay a dead-lettered job: remove it from the DLQ, reset its attempts and re-enqueue it.
--
-- KEYS[1] dead-letter list   KEYS[2] job hash   KEYS[3] stream   KEYS[4] priority zset
-- ARGV[1] job id             ARGV[2] member     ARGV[3] high-priority threshold
-- ARGV[4] event channel
--
-- Returns 1 if replayed, 0 if the job was not in the DLQ, -1 if the job hash is gone.

if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end

local now = now_ms()
redis.call('HSET', KEYS[2], 'status', 'pending', 'attempts', '0', 'last_error', '', 'worker_id', '')
redis.call('HDEL', KEYS[2], 'completed_at', 'started_at', 'next_retry_at')
redis.call('PERSIST', KEYS[2])
route(KEYS[2], KEYS[3], KEYS[4], ARGV[2], ARGV[1], ARGV[3], now)

local info = redis.call('HMGET', KEYS[2], 'type', 'queue')
emit(ARGV[4], {type = 'job.replayed', job_id = ARGV[1], job_type = info[1], queue = info[2]}, now)
return 1
