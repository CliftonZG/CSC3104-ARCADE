-- commit.lua: fenced snapshot write.
--
-- Atomically checks the writer's fencing token (epoch) against the match's
-- authoritative epoch and only then persists the snapshot. Because Redis runs
-- scripts single-threaded, no other write or epoch claim can interleave
-- between the check and the write.
--
-- KEYS[1]  epoch key     (string counter)
-- KEYS[2]  snapshot key  (hash: data, format, seq, epoch, ts_ms)
-- KEYS[3]  stream key    (capped stream of every committed snapshot)
--
-- ARGV[1]  writer epoch (fencing token held by the pod)
-- ARGV[2]  state sequence number
-- ARGV[3]  snapshot format ("json" | "binary")
-- ARGV[4]  snapshot payload
-- ARGV[5]  stream max length (0 disables the stream)
--
-- Returns {status, value}:
--   {"OK",     ts_ms}         snapshot committed at Redis server time ts_ms
--   {"DUP",    ts_ms}         same epoch + seq already committed (idempotent retry)
--   {"FENCED", current_epoch} writer epoch is not the current epoch
--   {"STALE",  stored_seq}    a newer sequence is already committed

local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local epoch = tonumber(ARGV[1])
if epoch ~= current then
  return {'FENCED', current}
end

local seq = tonumber(ARGV[2])
local stored = redis.call('HMGET', KEYS[2], 'seq', 'epoch', 'ts_ms')
local storedSeq = tonumber(stored[1] or '-1')
local storedEpoch = tonumber(stored[2] or '-1')

if seq == storedSeq and epoch == storedEpoch then
  return {'DUP', stored[3]}
end
if seq <= storedSeq then
  return {'STALE', storedSeq}
end

-- Use the Redis server clock so every pod measures snapshot age against the
-- same clock, regardless of node clock skew.
local t = redis.call('TIME')
local tsMs = t[1] .. string.format('%03d', math.floor(tonumber(t[2]) / 1000))

redis.call('HSET', KEYS[2],
  'data', ARGV[4],
  'format', ARGV[3],
  'seq', ARGV[2],
  'epoch', ARGV[1],
  'ts_ms', tsMs)

local maxLen = tonumber(ARGV[5])
if maxLen > 0 then
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', maxLen, '*',
    'seq', ARGV[2],
    'epoch', ARGV[1],
    'format', ARGV[3],
    'data', ARGV[4])
end

return {'OK', tsMs}
