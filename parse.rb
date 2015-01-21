require 'socket'
require 'redis'
require 'json'

def update_plane(planes, updates, id, info)
  if(planes[id].length < info.length)
    planes[id].replace(info)
  else
    for i in 2..planes[id].length do
      if(info[i] != '' && info[i] != nil && planes[id][i] != info[i])
        planes[id][i] = info[i]
        updates[id] = true
      end
    end
  end
end

def update_redis(redis, planes, updates)
  planes.each do |key, value|
    complete = true
    [11,14,15].each do |item|
      complete = false if value[item] == '' || value[item] == nil
    end
    if complete && updates[value[4].to_sym]
      redis.set(key.to_s, value.to_json, {:ex => 360})
      updates[value[4].to_sym] = false
      puts "updated redis"
    end
    #puts key.to_s+" : "+value.to_s if complete
  end
end

def prune_hash(redis, planes, updates)
  planes.delete_if do |key, value|
    result = redis.get(key)
    if result == nil
      updates.delete(key)
      puts "Pruned key: #{key}"
      return true
    end
    return false
  end
end


s = TCPSocket.new 'localhost', 30003
planes = Hash.new
updates = Hash.new
redis = Redis.new
times = 0

while line = s.gets
  line.strip!
  parts = line.split(',')
  next if parts[1] == '8'
  if !planes.has_key?(parts[4].to_sym) then
    planes[parts[4].to_sym] = parts
    updates[parts[4].to_sym] = false
  else
    update_plane(planes, updates, parts[4].to_sym, parts)
  end
  update_redis(redis, planes, updates)
  if times >= 10000
    puts "Hash pruning"
    prune_hash(redis, planes, updates)
    times = 0
  end
  times +=1
end

s.close