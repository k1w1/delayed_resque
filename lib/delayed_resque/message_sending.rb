require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::BasicObject
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def method_missing(method, *args)
      performable = @payload_class.new(@target, method.to_sym, @options, args)
      if @options[:at]
        ::Resque.enqueue_at_with_queue(@options[:queue], @options[:at], @payload_class, performable.store) 
      elsif @options[:in]
        ::Resque.enqueue_in_with_queue(@options[:queue], @options[:in], @payload_class, performable.store)
      else
        ::Resque.enqueue_to(@options[:queue], @payload_class, performable.store)
      end
    end
  end
  
  
  module MessageSending
    def delay(options = {})
      DelayProxy.new(PerformableMethod, self, options)
    end
    alias __delay__ delay
  end
end