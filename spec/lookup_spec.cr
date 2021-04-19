require "./spec_helper"

describe RedisServiceManager::Lookup do
  it "should return a rendezvous-hash for the cluster" do
    channel = Channel(Nil).new
    leader = ""

    # Start a 2 node cluster
    node1 = RedisServiceManager.new("spec", "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready.should eq(false)
    node1.on_rebalance do |version, _nodes|
      puts "REBALANCING NODE 1"
      node1.ready(version)
    end
    node1.on_cluster_ready do |_version|
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end

    node2 = RedisServiceManager.new("spec", "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready.should eq(false)
    node2.on_rebalance do |version, _nodes|
      puts "REBALANCING NODE 2"
      node2.ready(version)
    end
    node2.on_cluster_ready do |_version|
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end
    node1.start
    Fiber.yield
    node2.start
    channel.receive?

    node1.cluster_size.should eq(2)
    node1.ready.should be_true
    node1.leader.should be_true
    node2.cluster_size.should eq(2)
    node2.ready.should be_true
    leader.should eq("node1")

    # Get the cluster state
    lookup = RedisServiceManager::Lookup.new("spec")
    redis = Redis::Client.boot(REDIS_URL)
    nodes = lookup.nodes(redis)
    nodes.nodes.should eq(["http://node1/node1", "http://node2/node2"])

    node2.stop
    node1.stop
  end
end
