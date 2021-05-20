require 'spec_helper'

RSpec.describe DelayedResque::MessageSending do
  around do |ex|
    travel_to(Time.current) { ex.run }
  end

  before { ResqueSpec.reset! }

  describe 'delayed class method' do
    class DummyObject
      include DelayedResque::MessageSending

      def self.first_method(param)
      end
    end

    subject(:delayed_method) do
      DummyObject.delay(job_options).first_method(*method_args)
    end

    let(:job_options) { {} }
    let(:method_args) { [123] }
    let(:method_name) { :first_method }
    let(:base_job_options) do
      {
        'obj' => 'CLASS:DummyObject',
        'method' => method_name,
        'args' => method_args
      }
    end
    let(:queue_name) { :default }

    it 'enqueues the job to the default queue' do
      delayed_method
      expect(DelayedResque::PerformableMethod).to have_queued(
        base_job_options.merge('t' => Time.current.to_f)
      ).in(queue_name)
    end

    context 'with custom queue' do
      let(:queue_name) { :not_default_queue }
      let(:job_options) { { queue: queue_name } }

      it 'enqueues the job to the custom queue' do
        delayed_method
        expect(DelayedResque::PerformableMethod).to have_queued(
          base_job_options.merge('t' => Time.current.to_f)
        ).in(queue_name)
      end
    end

    context 'when method is missing' do
      subject(:delayed_method) do
        DummyObject.delay(job_options).invalid_method_name(*method_args)
      end

      it 'raises an error' do
        expect { delayed_method }.to raise_error(NoMethodError)
      end
    end

    context 'when job is unique' do
      let(:job_options) { { unique: true } }

      let(:uuids) { Array.new(3) { SecureRandom.uuid } }

      before do
        uuids
        SecureRandom.stub(:uuid).and_return(*uuids)
      end

      context 'when it is the first occurrence' do
        it 'enqueues the job with the expected arguments' do
          delayed_method
          expect(DelayedResque::PerformableMethod).to have_queued(
            base_job_options.merge(DelayedResque::PerformableMethod::UNIQUE_JOB_ID => uuids.first)
          ).in(queue_name)
        end

        it 'tracks the unique job id' do
          delayed_method
          expect(
            Resque.redis.hget(
              DelayedResque::PerformableMethod::UNIQUE_JOBS_NAME,
              Resque.encode(base_job_options)
            )
          ).to eq(uuids.first)
        end
      end

      context 'when it is not the first occurrence' do
        before do
          # Force the first stub value to fire
          SecureRandom.uuid
          Resque.redis.hset(
            DelayedResque::PerformableMethod::UNIQUE_JOBS_NAME,
            Resque.encode(base_job_options),
            uuids.first
          )
          Resque.enqueue_to(
            queue_name,
            DelayedResque::PerformableMethod,
            base_job_options.merge(DelayedResque::PerformableMethod::UNIQUE_JOB_ID => uuids.first)
          )
        end

        it 'leaves the original job in the queue' do
          delayed_method
          expect(DelayedResque::PerformableMethod).to have_queued(
            base_job_options.merge(DelayedResque::PerformableMethod::UNIQUE_JOB_ID => uuids.first)
          ).in(queue_name)
        end

        it 'enqueues the job with the new unique id' do
          delayed_method
          expect(DelayedResque::PerformableMethod).to have_queued(
            base_job_options.merge(DelayedResque::PerformableMethod::UNIQUE_JOB_ID => uuids.second)
          ).in(queue_name)
        end

        it 'tracks the unique job id' do
          delayed_method
          expect(
            Resque.redis.hget(
              DelayedResque::PerformableMethod::UNIQUE_JOBS_NAME,
              Resque.encode(base_job_options)
            )
          ).to eq(uuids.second)
        end
      end
    end
  end
end
