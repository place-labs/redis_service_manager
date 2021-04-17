# Redis Service Manager

[![Build Status](https://travis-ci.com/place-labs/redis_service_manager.svg?branch=master)](https://travis-ci.com/github/place-labs/redis_service_manager)

Maintains a list of services in a cluster and tracks their ready state after a rebalance.
For use in crystal lang projects.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     redis_service_manager:
       github: place-labs/redis_service_manager
   ```

2. Run `shards install`


## Usage

```crystal

require "redis_service_manager"

# TTL should be number > 3 that is not divisible by 3 to account for process load
# the manager will check-in with the cluster every `ttl // 3` seconds
manager = RedisServiceManager.new("service_name", "http://service:1234/location", redis: "redis://localhost:6379", ttl: 4)

# a rebalancing event occurs when a change to the cluster is detected
# the cluster will only become ready once all nodes have marked themselves as ready
# so make sure you call ready (critical function)
manager.on_rebalance do |version, nodes|
  puts "REBALANCING"
  manager.ready(version)
end

# this callback is only fired on the cluster leader
manager.on_cluster_ready do |version|
  puts "CLUSTER READY"
end

manager.start

```
