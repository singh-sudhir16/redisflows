-- Acknowledge a delivery without changing job state (duplicate or orphaned delivery).
--
-- KEYS[1] ack key   KEYS[2] lease hash
-- ARGV[1] ack kind  ARGV[2] ack ref   ARGV[3] consumer group

ack(ARGV[1], KEYS[1], KEYS[2], ARGV[2], ARGV[3])
return 1
