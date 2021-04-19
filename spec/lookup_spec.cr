require "./spec_helper"

describe RedisServiceManager do
  it "should return a rendezvous-hash for the cluster" do
    channel = Channel(Nil).new
    leader = ""

    # Start a 2 node cluster
    node1 = RedisServiceManager.new("spec", uri: "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready.should eq(false)
    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 1"
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable do
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end

    node2 = RedisServiceManager.new("spec", uri: "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready.should eq(false)
    node2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 2"
      rebalance_complete_cb.call
    end
    node2.on_cluster_stable do
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end

    node1.register
    node2.register
    loop do
      break if node1.cluster_size == 2
      channel.receive?
    end

    node1.cluster_size.should eq(2)
    node1.ready.should be_true
    node2.cluster_size.should eq(2)
    node2.ready.should be_true

    # Get the cluster state
    lookup = Clustering::Discovery.new RedisServiceManager.new("spec", REDIS_URL)
    hash = lookup.nodes
    hash.nodes.includes?("http://node1/node1").should be_true
    hash.nodes.includes?("http://node2/node2").should be_true

    node2.unregister
    node1.unregister
  end
end
