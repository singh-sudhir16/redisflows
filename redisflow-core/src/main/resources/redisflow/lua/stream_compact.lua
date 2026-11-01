-- Trim stream entries that every consumer group has already received and acknowledged.
--
-- XADD MAXLEN trimming can silently drop messages that were never delivered when a
-- backlog builds up. Instead, compute a safe point per group as the lower of its
-- last-delivered-id and its oldest pending entry, take the minimum across all groups,
-- and remove only entries strictly below it (XTRIM MINID).
--
-- KEYS[1] stream
-- Returns the number of entries removed.

if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end

local function parse(id)
  local ms, seq = string.match(id, '^(%d+)-(%d+)$')
  return tonumber(ms), tonumber(seq)
end

local function less(a, b)
  local am, as = parse(a)
  local bm, bs = parse(b)
  if am ~= bm then return am < bm end
  return as < bs
end

local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups == 0 then return 0 end

local safe = nil
for _, g in ipairs(groups) do
  local name, last = nil, nil
  for i = 1, #g, 2 do
    if g[i] == 'name' then name = g[i + 1] end
    if g[i] == 'last-delivered-id' then last = g[i + 1] end
  end
  local candidate = last
  local pending = redis.call('XPENDING', KEYS[1], name)
  if tonumber(pending[1]) > 0 and less(pending[2], candidate) then
    candidate = pending[2]
  end
  if safe == nil or less(candidate, safe) then safe = candidate end
end

if safe == nil or safe == '0-0' then return 0 end
return redis.call('XTRIM', KEYS[1], 'MINID', safe)
