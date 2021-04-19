require "log"
require "ulid"

class RedisServiceManager
  Log = ::Log.for("redis-service-manager")

  def initialize(@service : String, @uri : String, redis : String, @ttl : Int32 = 20)
    # @redis = Redis.new(url: redis)
    @redis = Redis::Client.boot(redis)
    @lock = Mutex.new(:reentrant)
    @version_key = "service_#{@service}_version"
    @hash_key = "service_#{@service}_lookup"
    @hash = NodeHash.new(@hash_key, @redis)
  end

  getter version : String = ""
  getter stopped : Bool = true
  getter leader : Bool = false
  getter ready : Bool = false
  getter cluster_ready : Bool = false

  getter ulid : String = ""
  getter hash_key : String = ""
  getter node_key : String = ""

  getter node_keys : Array(String) = [] of String

  def cluster_size
    node_keys.size
  end

  def cluster_nodes
    @hash.to_h
  end

  # Called when the cluster has changed (version, node list id => URI)
  def on_rebalance(&@on_rebalance : (String, Hash(String, String)) ->)
  end

  # Called on leader node when the cluster has stabilised
  def on_cluster_ready(&@on_cluster_ready : String ->)
  end

  def start
    @lock.synchronize do
      return unless @stopped
      @stopped = false
      spawn do
        Log.trace { "node started" }
        maintain_registration
      end
    end
  end

  def stop : Nil
    @lock.synchronize do
      @stopped = true

      # remove node and bump the version
      @redis.del(node_key)
      @hash.delete(node_key)
      @redis.set(@version_key, ULID.generate)
    end

    Log.trace { "node stopped" }
  end

  def ready(version : String) : Nil
    # update this nodes registration if latest version is ready
    return unless version == @version
    @lock.synchronize do
      return unless version == @version

      @ready = true
      node_info = NodeInfo.new(@uri, version, ready: true)
      @redis.set(node_key, node_info.to_json, ex: @ttl)
    end

    Log.trace { "node ready on version #{version}" }
  end

  # Check if node is registered in the cluster
  # either join cluster or maintain registration
  def maintain_registration
    delay = @ttl // 3
    delay = 1 if delay <= 0

    loop do
      begin
        # get the current registration for this node and reset the timeout
        @lock.synchronize do
          break if stopped

          if node_info = @redis.getex(node_key, ex: @ttl)
            check_version(NodeInfo.from_json(node_info))
          else
            register
          end
        end
      rescue error
        Log.error(exception: error) { "node registration failed" }
      end

      sleep delay
    end
  end

  # Universally unique Lexicographically sortable IDentifiers
  protected def generate_ulid
    @leader = false
    @ulid = ULID.generate
    @node_key = "service_#{@service}.#{@ulid}"
  end

  protected def register
    generate_ulid
    node_info = NodeInfo.new(@uri, @version)

    Log.trace { "registering node #{@ulid} in cluster" }

    # register this node
    @redis.set(node_key, node_info.to_json, ex: @ttl)
    @hash[node_key] = @uri

    # expire the version
    @redis.set(@version_key, ULID.generate)

    check_version(node_info)
  end

  protected def check_version(node_info : NodeInfo) : Nil
    version = @redis.get(@version_key) || "not-set"
    new_list = get_new_node_list

    # check for new leader and update URIS
    if new_list != node_keys || node_info.version != version
      @cluster_ready = @ready = false
      @leader = new_list.first? == node_key

      Log.trace { "cluster update detected: node list changed #{new_list != node_keys}, version changed #{node_info.version != version}" }

      if @leader
        if node_info.version == version
          version = ULID.generate
          @redis.set(@version_key, version)
          Log.trace { "as leader, updating cluster to version #{version}" }
        end
      elsif node_info.version == version
        # wait for new version as not the leader
        Log.trace { "ignoring cluster change, waiting for leader to update version" }
        return
      end

      @node_keys = new_list
      @version = version

      # update this nodes registration
      node_info = NodeInfo.new(@uri, @version)
      @redis.set(node_key, node_info.to_json, ex: @ttl)

      # notify of rebalance
      Log.trace { "cluster details updated, #{new_list.size} node detected, requesting rebalance on version #{version} and waiting for ready" }
      perform_rebalance version

      # we'll give the node a tick before checking if cluster ready
      return
    end

    # leader is in charge of notifying the cluster
    check_cluster_ready(new_list, version) if @leader && @ready && !@cluster_ready
  end

  protected def check_cluster_ready(new_list, version)
    node_info = {} of String => NodeInfo

    Log.trace { "as leader, checking for node readiness" }

    # leader ensures the node hash is up to date
    new_version_required = false
    new_list.each do |node|
      if info_raw = @redis.get(node)
        info = NodeInfo.from_json(info_raw)
        node_info[node] = info
      else
        @hash.delete(node)
        new_version_required = true
        Log.trace { "as leader, removed expired node from lookup #{node}" }
      end
    end

    if new_version_required
      version = ULID.generate
      @redis.set(@version_key, version)
      Log.trace { "as leader, updating cluster to version #{version}" }
      return
    end

    # check node versions are all up to date
    node_info.each do |node, info|
      if info.version != version
        Log.trace { "as leader, out of date node #{node} => #{info.uri} - node version #{info.version} != current version #{version}" }
        return
      end
    end

    # check for cluster ready
    node_info.each do |node, info|
      if !info.ready
        Log.trace { "as leader, node is not ready: #{node} => #{info.uri}" }
        return
      end
    end

    # update ready state
    Log.trace { "as leader, cluster is ready, all nodes are reporting ready" }
    @cluster_ready = true
    if cluster_ready_cb = @on_cluster_ready
      spawn { cluster_ready_cb.call(version) }
    end
  end

  protected def perform_rebalance(version : String)
    if rebalance_cb = @on_rebalance
      node_map = @hash.to_h
      spawn { rebalance_cb.call(version, node_map) }
    end
  end

  protected def get_new_node_list
    new_list = @hash.keys

    # find the leader, as the old leader might be offline
    delete = [] of String
    leader = false
    new_list.each do |node|
      if @redis.get(node).nil?
        delete << node
        @hash.delete(node)
      elsif !leader
        # Leader node should validate the whole cluster
        leader = node == node_key
        break unless leader
      end
    end

    new_list - delete
  end
end

require "./redis_service_manager/*"
