require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::BasicObject
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def method_missing(method, *args)
      queue = @options[:queue] || @payload_class.queue
      performable = @payload_class.new(@target, method.to_sym, @options, args)
      
      if @options[:unique]
        if @options[:at] or @options[:in]
          ::Resque.remove_delayed(@payload_class, performable.store)
        else
          ::Resque.dequeue(@payload_class, performable.store)
        end
      end
      
      if @options[:at]
        ::Resque.enqueue_at(@options[:at], @payload_class, performable.store) 
      elsif @options[:in]
        ::Resque.enqueue_in(@options[:in], @payload_class, performable.store)
      else
        ::Resque.enqueue_to(queue, @payload_class, performable.store)
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