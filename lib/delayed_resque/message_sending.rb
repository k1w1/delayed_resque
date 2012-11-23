module DelayedResque
  class DelayProxy < ActiveSupport::BasicObject
    def initialize(target, options)
      @target = target
      @options = {:queue => :default}.update(options.symbolize_keys)
    end

    def method_missing(method, *args)
      performable = PerformableMethod.new(@target, method.to_sym, args)
      ::Resque.enqueue_to(@options[:queue], PerformableMethod, *performable.dump_args)
    end
  end
  
  
  module MessageSending
    def delay(options = {})
      DelayProxy.new(self, options)
    end
    alias __delay__ delay
  end
end