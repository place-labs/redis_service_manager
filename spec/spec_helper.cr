require "spec"
require "../src/redis_service_manager"

REDIS_HOST = ENV["REDIS_HOST"]? || "localhost"
REDIS_PORT = (ENV["REDIS_PORT"]? || "6379").to_i
REDIS_URL  = "redis://#{REDIS_HOST}:#{REDIS_PORT}"

# re-open class to add some chaos, not for production use
class RedisServiceManager
  def chaos_stop : Nil
    @lock.synchronize { @registered = false }
    Log.fatal { "CHAOS node stopped unceremoniously" }
  end
end

::Log.setup("*", :trace)

Spec.before_suite do
  ::Log.builder.bind("*", backend: ::Log::IOBackend.new(STDOUT), level: ::Log::Severity::Trace)
end

Spec.before_each do
  puts "\n---------------------------------\n\n"
end
