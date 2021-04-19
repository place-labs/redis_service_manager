require "./node_hash"
require "rendezvous-hash"

# NOTE:: this file can be required without the rest of RedisServiceManager
class RedisServiceManager
  class Lookup
    def initialize(service : String, @ttl : Time::Span = 5.seconds)
      @nodes = RendezvousHash.new
      @expires = Time.unix(0)
      @hash_key = "{service_#{service}}_lookup"
    end

    # Redis is passed in here as it's not threadsafe and this way we can have a
    # single redis client in use for services that just need to discover which
    # core a module is running on
    def nodes(redis : Redis::Client, fresh : Bool = false)
      return @nodes unless fresh || Time.utc >= @expires

      hash = NodeHash.new(@hash_key, redis).to_h
      keys = hash.keys.sort!
      nodes = RendezvousHash.new(nodes: keys.map { |key| hash[key] })
      @expires = @ttl.from_now
      @nodes = nodes
    end
  end
end
