require 'socket'
require 'redis'
require 'json'

def update_plane(planes, id, info)
  if(planes[id].length < info.length)
    planes[id].replace(info)
  else
    for i in 2..planes[id].length do
      if(info[i] != '' && info[i] != nil && planes[id][i] != info[i])
        planes[id][i] = info[i]
      end
    end
  end
end

def update_redis(redis, planes)
  planes.each do |key, value|
    complete = true
    [11,14,15].each do |item|
      complete = false if value[item] == '' || value[item] == nil
    end
    redis.set(key.to_s, value.to_json, {:ex => 360}) if complete
    #puts key.to_s+" : "+value.to_s if complete
  end
end

s = TCPSocket.new 'localhost', 30003
planes = Hash.new
redis = Redis.new

while line = s.gets
  line.strip!
  parts = line.split(',')
  next if parts[1] == '8'
  if !planes.has_key?(parts[4].to_sym) then
    planes[parts[4].to_sym] = parts
  else
    update_plane(planes, parts[4].to_sym, parts)
  end
  update_redis(redis, planes)
end

s.close