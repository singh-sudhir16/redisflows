-- Record a visit in the daily HyperLogLogs in one round trip.
-- (A script rather than a client-side pipeline: pipelines need a dedicated pooled connection,
-- which under high request concurrency turns into pool contention on the hot path.)
--
-- KEYS[1] visitors HLL   KEYS[2] unique-IP HLL
-- ARGV[1] visitor id ('' = skip)   ARGV[2] ip ('' = skip)   ARGV[3] retention seconds

if ARGV[1] ~= '' then
  redis.call('PFADD', KEYS[1], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[3])
end
if ARGV[2] ~= '' then
  redis.call('PFADD', KEYS[2], ARGV[2])
  redis.call('EXPIRE', KEYS[2], ARGV[3])
end
return 1
