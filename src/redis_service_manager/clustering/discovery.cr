require "uri"
require "../clustering"

class Clustering::Discovery
  def initialize(@cluster : Clustering, @ttl : Time::Span = 5.seconds)
    @rendezvous = RendezvousHash.new
    @last_updated = Time.unix(0)
    @timer = Time.monotonic - (@ttl + 1.second)
    @mutex = Mutex.new
    @rebalance_callbacks = [] of RendezvousHash ->
    @cluster.on_rebalance { |nodes, _rebalance_complete_cb| @mutex.synchronize { update_node_list(nodes) } }
  end

  getter last_updated : Time
  getter rebalance_callbacks : Array(RendezvousHash ->)
  getter uri : URI { URI.parse @cluster.uri }

  def on_rebalance(&callback : RendezvousHash ->)
    @rebalance_callbacks << callback
  end

  def rendezvous : RendezvousHash
    if @cluster.watching?
      @cluster.rendezvous
    elsif !expired?
      # this is up to date as we are listening to cluster.on_rebalance callback
      # or the current list hasn't had its TTL expire
      @rendezvous
    else
      @mutex.synchronize do
        if expired?
          @rendezvous = @cluster.rendezvous
          @last_updated = Time.utc
          @timer = Time.monotonic
        end
      end
      @rendezvous
    end
  end

  # Consistent hash lookup
  def find?(key : String) : URI?
    rendezvous.find?(key).try { |node| URI.parse(node) }
  end

  # Consistent hash lookup
  def find(key : String) : URI
    URI.parse(rendezvous.find(key))
  end

  def [](key)
    find(key)
  end

  def []?(key)
    find?(key)
  end

  # Determine if key maps to current node
  def own_node?(key : String) : Bool
    # don't parse into URI
    rendezvous.find?(key) == @cluster.uri
  end

  # Returns the list of node URIs from the `rendezvous-hash`
  def nodes : Array(URI)
    rendezvous.nodes.map { |node| URI.parse(node) }
  end

  protected def update_node_list(rendezvous : RendezvousHash)
    @rendezvous = rendezvous
    @last_updated = Time.utc
    @timer = Time.monotonic
    rebalance_callbacks.each { |callback| spawn { perform(callback, rendezvous) } }
  end

  protected def perform(callback, rendezvous)
    callback.call(rendezvous)
  rescue error
    Log.error(exception: error) { "rebalance callback failed" }
  end

  protected def expired?
    elapsed = Time.monotonic - @timer
    elapsed > @ttl
  end
end
