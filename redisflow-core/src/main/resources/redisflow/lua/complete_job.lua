-- Mark a job completed and acknowledge its delivery in one atomic step.
--
-- KEYS[1] job hash   KEYS[2] ack key (stream | priority in-flight zset)
-- KEYS[3] lease hash KEYS[4] stats hash
--
-- ARGV[1] ack kind ('stream' | 'lease')   ARGV[2] ack ref (message id | member)
-- ARGV[3] consumer group                  ARGV[4] retention ms for completed jobs (0 = keep)
-- ARGV[5] worker id                       ARGV[6] event channel
--
-- Returns 1 if the job transitioned to completed, 0 if it was already terminal or missing.

ack(ARGV[1], KEYS[2], KEYS[3], ARGV[2], ARGV[3])

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' or status == 'dead' then return 0 end

local now = now_ms()
redis.call('HSET', KEYS[1], 'status', 'completed', 'completed_at', int(now), 'last_error', '')
local retention = tonumber(ARGV[4])
if retention > 0 then redis.call('PEXPIRE', KEYS[1], retention) end
redis.call('HINCRBY', KEYS[4], 'completed', 1)

local info = redis.call('HMGET', KEYS[1], 'id', 'type', 'queue', 'attempts', 'started_at')
local duration = 0
if info[5] then duration = now - tonumber(info[5]) end
emit(ARGV[6], {type = 'job.completed', job_id = info[1], job_type = info[2], queue = info[3],
               worker_id = ARGV[5], attempt = tonumber(info[4]), duration_ms = duration}, now)
return 1
