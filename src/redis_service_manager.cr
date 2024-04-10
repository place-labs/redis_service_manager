require "log"
require "ulid"
require "./redis_service_manager/clustering"

class RedisServiceManager < Clustering
  Log = ::Log.for("redis-service-manager")

  def initialize(service : String, redis : String, @uri : String = "", @ttl : Int32 = 20)
    super(service)

    @redis = Redis::Client.boot(redis)
    @lock = Mutex.new(:reentrant)
    @version_key = "{service_#{service}}_version"
    @hash_key = "{service_#{service}}_lookup"
    @hash = NodeHash.new(@hash_key, @redis)
    @rendezvous_hash = RendezvousHash.new
    @node_info = NodeInfo.new(@uri, "")
  end

  def initialize(service : String, @redis : Redis::Client, @uri : String = "", @ttl : Int32 = 20, @lock : Mutex = Mutex.new(:reentrant))
    super(service)

    @version_key = "{service_#{service}}_version"
    @hash_key = "{service_#{service}}_lookup"
    @hash = NodeHash.new(@hash_key, @redis)
    @rendezvous_hash = RendezvousHash.new
    @node_info = NodeInfo.new(@uri, "")
  end

  getter version : String = ""
  getter? registered : Bool = false
  getter? watching : Bool = false

  getter? leader : Bool = false
  getter? ready : Bool = false
  getter? cluster_ready : Bool = false

  getter uri : String
  getter ulid : String = ""
  getter hash_key : String = ""
  getter node_key : String = ""
  getter node_info : NodeInfo

  getter node_keys : Array(String) = [] of String

  def cluster_size
    node_keys.size
  end

  def cluster_nodes
    @hash.to_h
  end

  def register : Bool
    @lock.synchronize do
      return false if @registered
      @registered = true
      @watching = true
      spawn do
        Log.trace { "node started" }
        registration_maintenance_loop
      end
    end
    true
  end

  def unregister : Bool
    @lock.synchronize do
      return false unless @registered
      @registered = false
      @watching = false

      # remove node and bump the version
      @redis.multi(node_key, reconnect: true) do |transaction|
        transaction.del(node_key)
        # @hash.delete(node_key)
        transaction.hdel(hash_key, node_key)
        transaction.set(@version_key, ULID.generate)
      end
    end

    Log.trace { "node stopped" }
    true
  end

  protected def ready(version : String) : Nil
    # update this nodes registration if latest version is ready
    @lock.synchronize do
      return unless version == @version

      @ready = true
      @node_info = node_info = NodeInfo.new(@uri, version, ready: true)
      @redis.set(node_key, node_info.to_json, ex: @ttl)
    end

    Log.trace { "node ready on version #{version}" }
  end

  # Check if node is registered in the cluster
  # either join cluster or maintain registration
  def maintain_registration
    # get the current registration for this node and reset the timeout
    @lock.synchronize do
      return unless registered?

      if @redis.getex(node_key, ex: @ttl)
        check_version(@node_info)
      else
        perform_registration
      end
    end
  rescue error
    Log.error(exception: error) { "node registration failed" }
  end

  # this loop ensures all nodes in the cluster are aware of each other
  # however registration maintenance can happen independently
  protected def registration_maintenance_loop
    delay = @ttl // 3
    delay = 1 if delay <= 0

    loop do
      maintain_registration
      sleep delay
      break unless registered?
    end
  end

  # Universally unique Lexicographically sortable IDentifiers
  protected def generate_ulid
    @leader = false
    @ulid = ULID.generate
    @node_key = "{service_#{@service}}.#{@ulid}"
  end

  protected def perform_registration
    generate_ulid
    @node_info = node_info = NodeInfo.new(@uri, @version)

    Log.trace { "registering node #{@ulid} in cluster" }

    # register this node
    @redis.multi(node_key, reconnect: true) do |transaction|
      transaction.set(node_key, node_info.to_json, ex: @ttl)
      transaction.hset(hash_key, node_key, @uri)
      # expire the version
      transaction.set(@version_key, ULID.generate)
    end

    check_version(node_info)
  end

  protected def check_version(node_info : NodeInfo) : Nil
    if version = @redis.get(@version_key)
      if node_info.version != version || leader?
        # perform a check of all nodes
        update_node_lists(version)
      elsif @redis.get(node_keys.first).nil?
        # the leader has gone offline
        update_node_lists(version)
      end
    else
      update_node_lists("no-version")
    end
  end

  protected def update_node_lists(version)
    hash, new_list = get_new_node_list
    return unless hash

    # check for new leader and update URIS
    if new_list != node_keys || node_info.version != version
      @cluster_ready = @ready = false
      @leader = new_list.first? == node_key

      Log.trace { "cluster update detected: node list changed #{new_list != node_keys}, version changed #{node_info.version != version}" }

      if @leader
        if node_info.version == version
          version = ULID.generate
          @redis.set(@version_key, version)
          Log.trace { "as leader #{@uri}, updating cluster to version #{version}" }
        end
      elsif node_info.version == version
        # wait for new version as not the leader
        Log.trace { "ignoring cluster change, waiting for leader to update version" }
        return
      end

      @node_keys = new_list
      @version = version

      # update this nodes registration
      @node_info = node_info = NodeInfo.new(@uri, version)
      @redis.set(node_key, node_info.to_json, ex: @ttl)

      # notify of rebalance
      Log.trace { "cluster details updated, #{new_list.size} nodes detected, requesting rebalance on version #{version} and waiting for ready" }
      perform_rebalance version, hash, new_list

      # we'll give the node a tick before checking if cluster ready
      return
    end

    # leader is in charge of notifying the cluster
    check_cluster_ready(new_list, version) if @leader && @ready && !@cluster_ready
  end

  protected def check_cluster_ready(new_list, version)
    node_info = {} of String => NodeInfo

    Log.trace { "as leader #{@uri}, checking for node readiness" }

    # leader ensures the node hash is up to date
    new_version_required = false
    new_list.each do |node|
      if info_raw = @redis.get(node)
        info = NodeInfo.from_json(info_raw)
        node_info[node] = info
      else
        @hash.delete(node)
        new_version_required = true
        Log.trace { "as leader #{@uri}, removed expired node from lookup #{node}" }
      end
    end

    if new_version_required
      version = ULID.generate
      @redis.set(@version_key, version)
      Log.trace { "as leader #{@uri}, updating cluster to version #{version}" }
      return
    end

    # check node versions are all up to date
    node_info.each do |node, info|
      if info.version != version
        Log.trace { "as leader #{@uri}, out of date node #{node} => #{info.uri} - node version #{info.version} != current version #{version}" }
        return
      end
    end

    # check for cluster ready
    node_info.each do |node, info|
      if !info.ready?
        Log.trace { "as leader #{@uri}, node is not ready: #{node} => #{info.uri}" }
        return
      end
    end

    # update ready state
    Log.trace { "as leader #{@uri}, cluster is ready, all nodes are reporting ready" }
    @cluster_ready = true
    cluster_stable_callbacks.each do |callback|
      spawn do
        begin
          callback.call
        rescue error
          Log.error(exception: error) { "notifying cluster stable" }
        end
      end
    end
  end

  protected def perform_rebalance(version : String, hash, new_list)
    the_nodes = [] of String
    new_list.each do |key|
      if uri = hash[key]?
        the_nodes << uri
      end
    end
    @rendezvous_hash = RendezvousHash.new(the_nodes)
    ready_cb = Proc(Nil).new { ready(version) }
    rebalance_callbacks.each do |callback|
      spawn do
        begin
          callback.call(@rendezvous_hash, ready_cb)
        rescue error
          Log.error(exception: error) { "performing rebalance callback" }
        end
      end
    end
  end

  protected def get_new_node_list
    hash = @hash.to_h
    new_list = hash.keys.sort!

    # find the leader, as the old leader might be offline
    delete = [] of String
    leader = false
    new_list.each do |node|
      if @redis.get(node).nil?
        delete << node
        @hash.delete(node)
        hash.delete(node)
      elsif !leader
        # Leader node should validate the whole cluster
        leader = node == node_key
        break unless leader
      end
    end

    # ensure this node is in the node list
    if hash[node_key]? != @uri
      Log.warn { "registration lost for #{@uri}, re-registering" }
      perform_registration
      return {nil, new_list}
    end

    {hash, new_list - delete}
  end

  def nodes : RendezvousHash
    if registered?
      @rendezvous_hash
    else
      hash = @lock.synchronize { @hash.to_h }
      keys = hash.keys.sort!
      RendezvousHash.new(nodes: keys.map { |key| hash[key] })
    end
  end
end

require "./redis_service_manager/*"
