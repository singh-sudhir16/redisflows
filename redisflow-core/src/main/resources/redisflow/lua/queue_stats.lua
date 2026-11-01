-- Snapshot one queue's health in a single round trip.
--
-- KEYS[1] stream   KEYS[2] dead-letter list
-- ARGV[1] consumer group
--
-- Returns {stream_length, backlog (not yet delivered), pending (delivered, unacked),
--          consumers, dead_letters}. Backlog is -1 when Redis cannot determine lag.

local length = redis.call('XLEN', KEYS[1])
local dead = redis.call('LLEN', KEYS[2])
local backlog, pending, consumers = length, 0, 0

if length > 0 or redis.call('EXISTS', KEYS[1]) == 1 then
  local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
  for _, g in ipairs(groups) do
    local fields = {}
    for i = 1, #g, 2 do fields[g[i]] = g[i + 1] end
    if fields['name'] == ARGV[1] then
      pending = fields['pending'] or 0
      consumers = fields['consumers'] or 0
      if fields['lag'] == false or fields['lag'] == nil then
        backlog = -1
      else
        backlog = fields['lag']
      end
    end
  end
end

return {length, backlog, pending, consumers, dead}
