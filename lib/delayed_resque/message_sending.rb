require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::BasicObject
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def method_missing(method, *args)
      performable = @payload_class.new(@target, method.to_sym, args)
      ::Resque.enqueue_to(@options[:queue], @payload_class, performable.store)
    end
  end
  
  
  module MessageSending
    def delay(options = {})
      DelayProxy.new(PerformableMethod, self, options)
    end
    alias __delay__ delay
  end
end