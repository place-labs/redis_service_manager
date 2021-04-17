require "./spec_helper"

describe RedisServiceManager do
  it "should join a cluster of one" do
    channel = Channel(Nil).new

    manager = RedisServiceManager.new("spec", "http://localhost:1234/spec1", redis: REDIS_URL, ttl: 4)
    manager.on_rebalance do |version, _nodes|
      puts "REBALANCING"
      manager.ready(version)
    end
    manager.on_cluster_ready do |_version|
      puts "CLUSTER READY"
      channel.close
    end
    manager.start

    channel.receive?
    manager.stop
  end

  it "should join a cluster of one and then rebalance when a new node joins" do
    channel = Channel(Nil).new
    leader = ""

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
    node1.start

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready.should eq(true)

    # Join a second node
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
    node2.start

    channel.receive?

    node1.cluster_size.should eq(2)
    node1.ready.should be_true
    node1.leader.should be_true
    node1.cluster_ready.should be_true

    node2.cluster_size.should eq(2)
    node2.ready.should be_true
    node2.leader.should be_false
    leader.should eq("node1")

    node1.stop
    channel.receive?

    node2.cluster_size.should eq(1)
    node2.ready.should be_true
    node2.leader.should be_true
    node2.cluster_ready.should be_true
    leader.should eq("node2")

    node2.stop
  end

  it "a two node cluster should detect when a node goes offline" do
    channel = Channel(Nil).new
    leader = ""

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
    node1.start

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready.should eq(true)

    # Join a second node
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
    node2.start

    channel.receive?

    node1.cluster_size.should eq(2)
    node1.ready.should be_true
    node1.leader.should be_true
    node1.cluster_ready.should be_true

    node2.cluster_size.should eq(2)
    node2.ready.should be_true
    node2.leader.should be_false
    leader.should eq("node1")

    node1.chaos_stop
    channel.receive?

    node2.cluster_size.should eq(1)
    node2.ready.should be_true
    node2.leader.should be_true
    node2.cluster_ready.should be_true
    leader.should eq("node2")

    node2.stop
  end

  it "should handle a node going offline and a new node replacing it" do
    channel = Channel(Nil).new
    leader = ""

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
    node1.start

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready.should eq(true)

    # Join a second node
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
    node2.start

    channel.receive?

    node1.cluster_size.should eq(2)
    node1.ready.should be_true
    node1.leader.should be_true
    node1.cluster_ready.should be_true

    node2.cluster_size.should eq(2)
    node2.ready.should be_true
    node2.leader.should be_false
    leader.should eq("node1")

    # ======
    # node1 goes offline and node3 replaces it
    # ======
    node1.chaos_stop
    node3 = RedisServiceManager.new("spec", "http://node3/node3", redis: REDIS_URL, ttl: 4)
    node3.ready.should eq(false)
    node3.on_rebalance do |version, _nodes|
      puts "REBALANCING NODE 3"
      sleep 10
      puts "REBALANCED NODE 3"
      node3.ready(version)
    end
    node3.on_cluster_ready do |_version|
      puts "CLUSTER READY NODE3"
      leader = "node3"
      channel.send nil
    end
    node3.start

    channel.receive?

    node2.cluster_size.should eq(2)
    node2.ready.should be_true
    node2.leader.should be_true
    node2.cluster_ready.should be_true
    leader.should eq("node2")

    node2.stop
    node3.stop
  end
end
