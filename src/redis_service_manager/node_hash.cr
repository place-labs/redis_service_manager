require "redis-cluster"

class RedisServiceManager
  class NodeHash < Hash(String, String)
    def initialize(@hash_key : String, @redis : Redis::Client)
      super()
    end

    getter redis : Redis::Client
    getter hash_key : String

    def []=(status_name, json_value)
      status_name = status_name.to_s
      adjusted_value = json_value.to_s.presence

      if adjusted_value
        redis.hset(hash_key, status_name, adjusted_value)
      else
        delete(status_name)
      end
      json_value
    end

    def fetch(key, &)
      key = key.to_s
      entry = redis.hget(hash_key, key)
      entry ? entry.to_s : yield key
    end

    def delete(key, &)
      key = key.to_s
      value = self[key]?
      if value
        redis.hdel(hash_key, key)
        return value.to_s
      end
      yield key
    end

    def keys
      redis.hkeys(hash_key).map(&.to_s).sort!
    end

    def values
      redis.hvals(hash_key).map &.to_s
    end

    def size
      redis.hlen(hash_key)
    end

    def empty?
      size == 0
    end

    def to_h : Hash(String, String)
      redis.hgetall(hash_key)
    end

    def clear
      hkey = hash_key
      keys = redis.hkeys(hkey)
      redis.pipelined(hkey, reconnect: true) do |pipeline|
        keys.each { |key| pipeline.hdel(hkey, key) }
      end
      self
    end
  end
end
