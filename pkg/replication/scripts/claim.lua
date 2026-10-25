-- claim.lua: compare-and-increment of the match epoch.
--
-- A pod that wants to become primary claims epoch N+1 only if the epoch is
-- still the N it last observed. Once the counter moves, every write carrying
-- an older token is rejected by commit.lua, which revokes the previous
-- primary's write access. The compare step guarantees that when two pods race
-- to promote from the same observed epoch, exactly one of them wins.
--
-- KEYS[1]  epoch key
-- ARGV[1]  epoch the caller expects to replace
--
-- Returns {status, value}:
--   {"OK",     new_epoch}
--   {"FENCED", current_epoch}  the epoch moved since the caller observed it

local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current ~= tonumber(ARGV[1]) then
  return {'FENCED', current}
end
return {'OK', redis.call('INCR', KEYS[1])}
