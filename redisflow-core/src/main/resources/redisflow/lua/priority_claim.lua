-- Pop up to N highest-priority jobs and lease them to the caller (visibility timeout).
-- A plain ZPOPMIN would lose the job if the worker crashed mid-processing; the lease
-- keeps it in an in-flight set until it is acknowledged or the lease expires.
--
-- KEYS[1] priority zset   KEYS[2] in-flight zset (score = lease deadline)   KEYS[3] lease hash
-- ARGV[1] max jobs        ARGV[2] lease duration ms
--
-- Returns the leased members ("queue:job_id"), highest priority first.

local items = redis.call('ZPOPMIN', KEYS[1], ARGV[1])
if #items == 0 then return {} end

local deadline = int(now_ms() + tonumber(ARGV[2]))
local leased = {}
for i = 1, #items, 2 do
  local member, score = items[i], items[i + 1]
  redis.call('ZADD', KEYS[2], deadline, member)
  redis.call('HSET', KEYS[3], member, score)  -- remember the original score for re-queueing
  leased[#leased + 1] = member
end
return leased
