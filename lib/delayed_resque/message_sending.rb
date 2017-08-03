require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::ProxyObject
    TRACKED_QUEUE_NAME = "trackedTasks"
    TRACKED_QUEUE_KEY = "tracked_task_key"
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def self.is_tracked?(key)
      ::Resque.redis.sismember(TRACKED_QUEUE_NAME, key)
    end

    def self.track_key(key)
      ::Resque.redis.sadd(TRACKED_QUEUE_NAME, key)
    end

    def method_missing(method, *args)
      queue = @options[:queue] || @payload_class.queue
      performable = @payload_class.new(@target, method.to_sym, @options, args)
      stored_options = performable.store
      
      if @options[:unique]
        if @options[:at] or @options[:in]
          ::Resque.remove_delayed(@payload_class, stored_options)
        else
          ::Resque.dequeue(@payload_class, stored_options)
        end
      end

      if @options[:tracked].present?
        ::DelayedResque::DelayProxy.track_key(@options[:tracked])
        stored_options[TRACKED_QUEUE_KEY] = @options[:tracked]
      end

      ::Rails.logger.warn("Queuing for RESQUE: #{stored_options['method']}: #{stored_options.inspect}")
      
      if @options[:at]
        ::Resque.enqueue_at(@options[:at], @payload_class, stored_options) 
      elsif @options[:in]
        ::Resque.enqueue_in(@options[:in], @payload_class, stored_options)
      else
        ::Resque.enqueue_to(queue, @payload_class, stored_options)
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
