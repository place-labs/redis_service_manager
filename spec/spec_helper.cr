require "spec"
require "../src/redis_service_manager"

# re-open class to add some chaos, not for production use
class RedisServiceManager
  def chaos_stop : Nil
    @lock.synchronize { @stopped = true }
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
