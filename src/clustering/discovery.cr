require "../clustering"

class Clustering::Discovery
  def initialize(@cluster : Clustering, @ttl : Time::Span = 5.seconds)
    @nodes = RendezvousHash.new
    @last_updated = Time.unix(0)
    @mutex = Mutex.new
    @rebalance_callbacks = [] of RendezvousHash ->
    @cluster.on_rebalance { |nodes, _version| update_node_list(nodes) }
  end

  getter last_updated : Time
  getter rebalance_callbacks : Array(RendezvousHash ->)

  def on_rebalance(&callback : RendezvousHash ->)
    @rebalance_callbacks << callback
  end

  def nodes : RendezvousHash
    if @cluster.watching? || !expired?
      # this is up to date as we are listening to cluster.on_rebalance callback
      # or the current list hasn't had its TTL expire
      @nodes
    else
      @mutex.synchronize do
        if expired?
          @nodes = @cluster.nodes
          @last_updated = Time.utc
        end
      end
      @nodes
    end
  end

  protected def update_node_list(nodes : RendezvousHash)
    @nodes = nodes
    @last_updated = Time.utc
    rebalance_callbacks.each { |callback| perform(callback, nodes) }
  end

  protected def perform(callback, nodes)
    callback.call(nodes)
  rescue error
    Log.error(exception: error) { "rebalance callback failed" }
  end

  protected def expired?
    expired = @last_updated + @ttl
    expired < Time.utc
  end
end