require 'rails'
require 'active_support'
require 'securerandom'

module DelayedResque
  class DelayProxy < ActiveSupport::ProxyObject
    TRACKED_QUEUE_NAME = "trackedTasks"
    TRACKED_QUEUE_KEY = "tracked_task_key"
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def self.tracked_task?(key)
      ::Resque.redis.sismember(TRACKED_QUEUE_NAME, key)
    end

    def self.track_task(key)
      ::Resque.redis.sadd(TRACKED_QUEUE_NAME, key)
    end

    def self.untrack_task(key)
      ::Resque.redis.srem(TRACKED_QUEUE_NAME, key)
    end

    def self.args_tracking_key(args)
      args_hash = Array(args).first
      args_hash[TRACKED_QUEUE_KEY].presence if args_hash.is_a?(::Hash)
    end

    def method_missing(method, *args)
      queue = @options[:queue] || @payload_class.queue
      @payload_class.with_queue(queue) do
        performable = @payload_class.new(@target, method.to_sym, @options, args)
        stored_options = performable.store

        if @options[:unique]
          if @options[:at] || @options[:in]
            ::Resque.remove_delayed(@payload_class, stored_options)
          else
            ::Resque.dequeue(@payload_class, stored_options)
          end
        elsif @options[:throttle]
          if @options[:at] || @options[:in]
            # This isn't perfect -- if a job is removed from the queue
            # but it takes a while to process it, we may have two jobs
            # scheduled N minutes apart, but actually run < N minutes
            # apart.
            return if ::Resque.delayed?(@payload_class, stored_options)
          else
            # Resque doesn't have a way to find a scheduled job, unless
            # you're deleting it. That's not what we want, because it
            # would have the same behavior as :unique -- just use that,
            # instead.
            ::Rails.logger.warn("Trying to throttle a non-scheduled job, which is unsupported.")
          end
        else
          # Ensure that the meta data is specific to the job.
          stored_options["unique_meta_id"] = ::SecureRandom.uuid
        end

        if @options[:tracked].present?
          ::DelayedResque::DelayProxy.track_task(@options[:tracked])
          stored_options[TRACKED_QUEUE_KEY] = @options[:tracked]
        end

        if @options[:meta].present?
          meta = @options.delete(:meta)
          ::DelayedResque::MetaData.store_meta_data(@payload_class, stored_options, meta)
        end

        ::Rails.logger.warn("Queuing for RESQUE: #{stored_options['method']}: #{stored_options.inspect}")

        if @options[:at]
          ::Resque.enqueue_at_with_queue(queue, @options[:at], @payload_class, stored_options)
        elsif @options[:in]
          ::Resque.enqueue_in_with_queue(queue, @options[:in], @payload_class, stored_options)
        else
          ::Resque.enqueue_to(queue, @payload_class, stored_options)
        end
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
