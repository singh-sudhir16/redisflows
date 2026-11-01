-- Move one due job (scheduled or awaiting retry) back to a runnable queue.
-- ZREM acts as the claim: if two dispatchers race, only one sees ZREM return 1,
-- and the removal and re-enqueue happen atomically so the job is never lost.
--
-- KEYS[1] source zset   KEYS[2] job hash   KEYS[3] stream   KEYS[4] priority zset
-- ARGV[1] member        ARGV[2] job id     ARGV[3] high-priority threshold
--
-- Returns 1 if dispatched, 0 if another dispatcher won, -1 if the job no longer exists.

if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end

redis.call('HSET', KEYS[2], 'status', 'pending')
route(KEYS[2], KEYS[3], KEYS[4], ARGV[1], ARGV[2], ARGV[3], now_ms())
return 1
