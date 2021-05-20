require 'rails'
require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::ProxyObject
    TRACKED_QUEUE_NAME = "trackedTasks"
    TRACKED_QUEUE_KEY = "tracked_task_key"

    UNIQUE_JOBS_NAME = "unique_jobs"

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

    # Returns an encoded string representing the job options that are used to
    # determine uniqueness when a job is enqueued with unique: true
    def self.unique_job_key(stored_options)
      # FYI - Redis has a limit of 512MB on the key size.
      ::Resque.encode(stored_options.except(PerformableMethod::UNIQUE_JOB_ID))
    end

    # The unique job id that was most recently enqueued for this set of job
    # options
    def self.last_unique_job_id(stored_options)
      ::Resque.redis.hget(UNIQUE_JOBS_NAME, unique_job_key(stored_options))
    end

    # Track each unique job so that we only execute it once
    def self.track_unique_job(stored_options)
      # We only care about tracking the last occurrence to be enqueued. The
      # hset will overwrite any previous value for this job key
      ::Resque.redis.hset(
        UNIQUE_JOBS_NAME,
        unique_job_key(stored_options),
        stored_options[PerformableMethod::UNIQUE_JOB_ID]
      )
    end

    def method_missing(method, *args)
      queue = @options[:queue] || @payload_class.queue
      performable = @payload_class.new(@target, method.to_sym, @options, args)
      stored_options = performable.store

      if @options[:tracked].present?
        ::DelayedResque::DelayProxy.track_task(@options[:tracked])
        stored_options[TRACKED_QUEUE_KEY] = @options[:tracked]
      end

      if @options[:unique]
        # remove_delayed uses @payload_class.queue rather than the queue
        # value within stored_options to determine whether or not a job
        # already exists. This can lead to issues when trying to remove
        # duplicates in a non-default queue
        @payload_class.with_queue(queue) do
          if @options[:at] || @options[:in]
            ::Resque.remove_delayed(@payload_class, stored_options)
          else
            # TODO: do we need to pass in the payload class here?
            # How about the queue? (I suspect the answer to both is yes)
            # Should this be in the same redis transaction as the RPUSH?
            ::DelayedResque::DelayProxy.track_unique_job(stored_options)
          end
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


  module MessageSending
    def delay(options = {})
      DelayProxy.new(PerformableMethod, self, options)
    end
    alias __delay__ delay
  end
end
