require "../redis_service_manager"
require "json"

class RedisServiceManager::NodeInfo
  include JSON::Serializable

  def initialize(@uri, @version, @ready = false)
  end

  @[JSON::Field(key: "u")]
  property uri : String

  @[JSON::Field(key: "r")]
  property? ready : Bool

  @[JSON::Field(key: "v")]
  property version : String
end
