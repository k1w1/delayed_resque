require 'active_record'

module DelayedResque
  class PerformableMethod < Struct.new(:object, :method, :args)
    CLASS_STRING_FORMAT = /^CLASS\:([A-Z][\w\:]+)$/
    AR_STRING_FORMAT = /^AR\:([A-Z][\w\:]+)\:(\d+)$/

    UNIQUE_JOBS_NAME = "unique_jobs"
    UNIQUE_JOB_ID = "job_uuid"

    def initialize(object, method, options, args)
      raise NoMethodError, "undefined method `#{method}' for #{object.inspect}" unless object.respond_to?(method)

      @object = dump(object)
      @method = method.to_sym
      @options = options
      @args = args.map { |a| dump(a) }
    end

    def display_name
      case self.object
      when CLASS_STRING_FORMAT then "#{$1}.#{method}"
      when AR_STRING_FORMAT then "#{$1}##{method}"
      else "Unknown##{method}"
      end
    end

    def unique_job_id
      return unless @options[:unique]

      @unique_job_id ||= ::SecureRandom.uuid
    end

    def queue
      @options[:queue] || self.class.queue
    end

    def self.queue
      @queue || "default"
    end

    def self.with_queue(queue)
      old_queue = @queue
      @queue = queue
      yield
    ensure
      @queue = old_queue
    end

    def self.around_perform_with_unique(options)
      if options[UNIQUE_JOB_ID]
        if options[UNIQUE_JOB_ID] != last_unique_job_id(options)
          ::Rails.logger.info("Ignoring duplicate copy of unique job. #{UNIQUE_JOB_ID}: #{options[UNIQUE_JOB_ID]}")
          return
        end

        untrack_unique_job(options)
      end

      yield options
    end

    def self.perform(options)
      object = options["obj"]
      method = options["method"]
      args = options["args"]
      arg_objects = []
      loaded_object = 
        begin
          arg_objects = args.map{|a| self.load(a)}
          self.load(object)
        rescue ActiveRecord::RecordNotFound
          Rails.logger.warn("PerformableMethod: failed to find record for #{object.inspect}")
          # We cannot do anything about objects which were deleted in the meantime
          return true
        end
      loaded_object.send(method, *arg_objects)
    end

    def store
      hsh = {"obj" => @object, "method" => @method, "args" => @args}.merge(@options[:params] || {})
      unless @options[:unique] || @options[:throttle] || @options[:at] || @options[:in]
        hsh["t"] = Time.now.to_f
      end

      hsh[UNIQUE_JOB_ID] = unique_job_id if @options[:unique]

      hsh
    end

    def track_unique_job
      return unless @options[:unique]

      # We only care about tracking the last occurrence to be enqueued. The
      # hset will overwrite any previous value for this job key
      ::Resque.redis.hset(
        UNIQUE_JOBS_NAME,
        unique_job_key,
        store[PerformableMethod::UNIQUE_JOB_ID]
      )
    end

    def self.untrack_unique_job(stored_options)
      return unless stored_options.key?(UNIQUE_JOB_ID)

      ::Resque.redis.hdel(
        UNIQUE_JOBS_NAME,
        unique_job_key(stored_options)
      )
    end

    # The unique job id that was most recently enqueued for this set of job
    # options
    def self.last_unique_job_id(stored_options)
      ::Resque.redis.hget(UNIQUE_JOBS_NAME, unique_job_key(stored_options))
    end

    private

    # Returns an encoded string representing the job options that are used to
    # determine uniqueness when a job is enqueued with unique: true
    def self.unique_job_key(stored_options)
      # FYI - Redis has a limit of 512MB on the key size.
      ::Resque.encode(stored_options.except(PerformableMethod::UNIQUE_JOB_ID))
    end

    def unique_job_key
      self.class.unique_job_key(store)
    end

    def self.load(arg)
      case arg
      when CLASS_STRING_FORMAT then $1.constantize
      when AR_STRING_FORMAT then $1.constantize.find($2)
      else arg
      end
    end

    def dump(arg)
      case arg
      when Class, Module then class_to_string(arg)
      when ActiveRecord::Base then ar_to_string(arg)
      else arg
      end
    end

    def ar_to_string(obj)
      "AR:#{obj.class}:#{obj.id}"
    end

    def class_to_string(obj)
      "CLASS:#{obj.name}"
    end
  end
end
