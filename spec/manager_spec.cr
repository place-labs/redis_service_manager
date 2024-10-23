require "./spec_helper"

describe RedisServiceManager do
  it "should join a cluster of one" do
    channel = Channel(Nil).new

    manager = RedisServiceManager.new("spec", uri: "http://localhost:1234/spec1", redis: REDIS_URL, ttl: 4)
    manager.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING"
      rebalance_complete_cb.call
    end
    manager.on_cluster_stable do
      puts "CLUSTER READY"
      channel.close
    end
    manager.register

    channel.receive?
    manager.unregister
  end

  it "should join a cluster of one and then rebalance when a new node joins" do
    channel = Channel(Nil).new
    leader = ""

    node1 = RedisServiceManager.new("spec", uri: "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready?.should eq(false)

    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 1"
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable do
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end
    node1.register

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready?.should eq(true)

    # Join a second node
    node2 = RedisServiceManager.new("spec", uri: "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready?.should eq(false)

    node2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 2"
      rebalance_complete_cb.call
    end
    node2.on_cluster_stable do
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end
    node2.register

    loop do
      break if node1.cluster_size == 2
      channel.receive?
    end

    node1.cluster_size.should eq(2)
    node1.ready?.should be_true
    node1.leader?.should be_true
    node1.cluster_ready?.should be_true

    node2.cluster_size.should eq(2)
    node2.ready?.should be_true
    node2.leader?.should be_false
    leader.should eq("node1")

    node1.unregister
    channel.receive?

    node2.cluster_size.should eq(1)
    node2.ready?.should be_true
    node2.leader?.should be_true
    node2.cluster_ready?.should be_true
    leader.should eq("node2")

    node2.unregister
  end

  it "a two node cluster should detect when the leader node goes offline" do
    channel = Channel(Nil).new
    leader = ""

    node1 = RedisServiceManager.new("spec", uri: "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready?.should eq(false)

    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 1"
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable do
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end
    node1.register

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready?.should eq(true)

    # Join a second node
    node2 = RedisServiceManager.new("spec", uri: "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready?.should eq(false)

    node2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 2"
      rebalance_complete_cb.call
    end
    node2.on_cluster_stable do
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end
    node2.register

    loop do
      break if node1.cluster_size == 2
      channel.receive?
    end

    node1.cluster_size.should eq(2)
    node1.ready?.should be_true
    node1.leader?.should be_true
    node1.cluster_ready?.should be_true

    node2.cluster_size.should eq(2)
    node2.ready?.should be_true
    node2.leader?.should be_false
    leader.should eq("node1")

    node1.chaos_stop
    channel.receive?

    node2.cluster_size.should eq(1)
    node2.ready?.should be_true
    node2.leader?.should be_true
    node2.cluster_ready?.should be_true
    leader.should eq("node2")

    node2.unregister
  end

  it "a two node cluster should detect when a node goes offline" do
    channel = Channel(Nil).new
    leader = ""

    node1 = RedisServiceManager.new("spec", uri: "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready?.should eq(false)

    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 1"
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable do
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end
    node1.register

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready?.should eq(true)

    # Join a second node
    node2 = RedisServiceManager.new("spec", uri: "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready?.should eq(false)

    node2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 2"
      rebalance_complete_cb.call
    end
    node2.on_cluster_stable do
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end
    node2.register

    loop do
      break if node1.cluster_size == 2
      channel.receive?
    end

    node1.cluster_size.should eq(2)
    node1.ready?.should be_true
    node1.leader?.should be_true
    node1.cluster_ready?.should be_true

    node2.cluster_size.should eq(2)
    node2.ready?.should be_true
    node2.leader?.should be_false
    leader.should eq("node1")

    node2.chaos_stop
    channel.receive?

    node1.cluster_size.should eq(1)
    node1.ready?.should be_true
    node1.leader?.should be_true
    node1.cluster_ready?.should be_true
    leader.should eq("node1")

    node1.unregister
  end

  it "should handle a node going offline and a new node replacing it" do
    channel = Channel(Nil).new
    leader = ""

    node1 = RedisServiceManager.new("spec", uri: "http://node1/node1", redis: REDIS_URL, ttl: 4)
    node1.ready?.should eq(false)

    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 1"
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable do
      puts "CLUSTER READY NODE1"
      leader = "node1"
      channel.send nil
    end
    node1.register

    channel.receive?
    node1.cluster_size.should eq(1)
    node1.ready?.should eq(true)

    # Join a second node
    node2 = RedisServiceManager.new("spec", uri: "http://node2/node2", redis: REDIS_URL, ttl: 4)
    node2.ready?.should eq(false)

    node2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 2"
      rebalance_complete_cb.call
    end
    node2.on_cluster_stable do
      puts "CLUSTER READY NODE2"
      leader = "node2"
      channel.send nil
    end
    node2.register

    loop do
      break if node1.cluster_size == 2
      channel.receive?
    end

    node1.cluster_size.should eq(2)
    node1.ready?.should be_true
    node1.leader?.should be_true
    node1.cluster_ready?.should be_true

    node2.cluster_size.should eq(2)
    node2.ready?.should be_true
    node2.leader?.should be_false
    leader.should eq("node1")

    # ======
    # node1 goes offline and node3 replaces it
    # ======
    node3 = RedisServiceManager.new("spec", uri: "http://node3/node3", redis: REDIS_URL, ttl: 4)
    node3.ready?.should eq(false)
    node3.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING NODE 3"
      sleep 10.seconds
      rebalance_complete_cb.call
    end
    node3.on_cluster_stable do
      puts "CLUSTER READY NODE3"
      leader = "node3"
      channel.send nil
    end

    node1.chaos_stop
    node3.register

    loop do
      break if node3.cluster_size == 2 && node2.cluster_size == 2
      channel.receive?
    end

    node2.cluster_size.should eq(2)
    node2.ready?.should be_true
    node2.leader?.should be_true
    node2.cluster_ready?.should be_true
    leader.should eq("node2")
    node2.node_hash.should eq({
      node2.ulid => URI.parse(node2.uri),
      node3.ulid => URI.parse(node3.uri),
    })

    node2.unregister
    node3.unregister
  end

  it "should run callbacks if it re-joins the cluster" do
    service_rand = rand(UInt16::MAX)

    # NODE 1
    channel1 = Channel(Nil).new
    manager = RedisServiceManager.new("rejoin#{service_rand}", uri: "http://localhost:1234/rejoin1", redis: REDIS_URL, ttl: 4)
    manager.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING 1"
      rebalance_complete_cb.call
    end
    manager.on_cluster_stable do
      puts "CLUSTER READY 1"
      channel1.send nil
    end
    manager.register
    channel1.receive?

    # NODE 2
    channel2 = Channel(Nil).new
    manager2 = RedisServiceManager.new("rejoin#{service_rand}", uri: "http://localhost:1234/rejoin2", redis: REDIS_URL, ttl: 4)
    manager2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING 2"
      rebalance_complete_cb.call
      channel2.send nil
    end
    manager2.on_cluster_stable do
      puts "CLUSTER READY 2 -- WARN: Not cluster primary"
    end
    manager2.register
    channel2.receive?
    channel1.receive?

    sleep 0.5.seconds
    manager2.simulate_crash
    channel2.close
    sleep 0.5.seconds

    # NODE 2 Re-establishes
    channel3 = Channel(Nil).new
    manager2 = RedisServiceManager.new("rejoin#{service_rand}", uri: "http://localhost:1234/rejoin2", redis: REDIS_URL, ttl: 4)
    manager2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING 2 RE"
      rebalance_complete_cb.call
      channel3.send nil
    end
    manager2.on_cluster_stable do
      puts "CLUSTER READY 2 RE -- WARN: Not cluster primary"
    end
    manager2.register

    select
    when channel3.receive
    when timeout(5.seconds)
      raise "rebalance failed...."
    end

    channel1.receive

    manager.unregister
    manager2.unregister
  end

  it "should recover from a split brain" do
    service_rand = rand(UInt16::MAX)
    send_signals = true

    # NODE 1
    channel1 = Channel(Nil).new
    manager = RedisServiceManager.new("rejoin#{service_rand}", uri: "http://localhost:1234/rejoin1", redis: REDIS_URL, ttl: 4)
    manager.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING 1 Split"
      rebalance_complete_cb.call
    end
    manager.on_cluster_stable do
      puts "CLUSTER READY 1 Split"
      channel1.send nil if send_signals
    end
    manager.register
    channel1.receive?

    # NODE 2
    channel2 = Channel(Nil).new
    manager2 = RedisServiceManager.new("rejoin#{service_rand}", uri: "http://localhost:1234/rejoin2", redis: REDIS_URL, ttl: 4)
    manager2.on_rebalance do |_nodes, rebalance_complete_cb|
      puts "REBALANCING 2 Split"
      rebalance_complete_cb.call
      channel2.send nil if send_signals
    end
    manager2.on_cluster_stable do
      puts "CLUSTER READY 2 Split -- WARN: Not cluster primary"
    end
    manager2.register
    channel2.receive?
    channel1.receive?

    sleep 0.5.seconds
    send_signals = false
    manager2.simulate_split_brain do
      send_signals = true
    end

    channel2.receive?
    channel1.receive?
  end
end
