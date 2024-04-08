require "redis-cluster"

class Redis
  module Commands
    def getex(key, ex = nil, px = nil, exat = nil, pxat = nil)
      q = ["GETEX", namespaced(key)]
      q << "EX" << ex.to_s if ex
      q << "PX" << px.to_s if px
      q << "EXAT" << exat.to_s if exat
      q << "PXAT" << pxat.to_s if pxat
      string_or_nil_command(q)
    end
  end

  module Cluster::Commands
    # ameba:disable Lint/UselessAssign
    proxy getex, key, ex = nil, px = nil, exat = nil, pxat = nil
  end
end
