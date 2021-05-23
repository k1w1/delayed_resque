require "active_support/concern"

module DelayedResque
  module UniqueJobs
    extend ActiveSupport::Concern

    UNIQUE_JOBS_NAME = "unique_jobs"
    UNIQUE_JOB_ID = "job_uuid"

    class_methods do
      # Track the last occurrence of a unique job to be enqueued
      def track_unique_job(stored_options)
        ::Resque.redis.hset(
          UNIQUE_JOBS_NAME,
          unique_job_key(stored_options),
          stored_options[UNIQUE_JOB_ID]
        )
      end

      # Untrack the last occurrence of a unique job to be enqueued
      def untrack_unique_job(stored_options)
        ::Resque.redis.hdel(
          UNIQUE_JOBS_NAME,
          unique_job_key(stored_options)
        )
      end

      # The unique job id that was most recently enqueued for this set of job
      # options
      def last_unique_job_id(stored_options)
        ::Resque.redis.hget(UNIQUE_JOBS_NAME, unique_job_key(stored_options))
      end

      # Returns true if stored_options are for a unique job
      def unique_job?(stored_options)
        stored_options[UNIQUE_JOB_ID].present?
      end

      # Returns true if stored_options are for the last enqueued occurrence of
      # the job
      def last_unique_job?(stored_options)
        stored_options[UNIQUE_JOB_ID] == last_unique_job_id(stored_options)
      end

      # Returns an encoded string representing the job options that are used to
      # determine uniqueness when a job is enqueued with unique: true
      def unique_job_key(stored_options)
        # FYI - Redis has a limit of 512MB on the key size.
        ::Resque.encode(stored_options.except(UNIQUE_JOB_ID))
      end
    end
  end
end
