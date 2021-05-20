require 'spec_helper'

RSpec.describe DelayedResque::PerformableMethod do
  include PerformJob

  class DummyObject
    @queue = :testqueue

    def self.do_something(*args)
      ::Kernel.puts("args: #{args.inspect}")
    end
  end

  around do |ex|
    travel_to(Time.current) { ex.run }
  end

  let(:redis) { ::Resque.redis }
  let(:object) { 'CLASS:DummyObject' }
  let(:method) { :do_something }
  let(:method_args) { [123] }
  let(:base_job_options) do
    {
      'obj' => object,
      'method' => method,
      'args' => method_args
    }
  end
  let(:additional_job_options) { {} }
  let(:options) { base_job_options.merge(additional_job_options) }
  let(:performable) do
    described_class.new(DummyObject, method, options, method_args)
  end

  describe '#store' do
    subject(:store) { performable.store }

    it 'has the correct obj' do
      expect(store).to include('obj' => object)
    end

    it 'has the correct method' do
      expect(store).to include('method' => method)
    end

    it 'has the correct args' do
      expect(store).to include('args' => method_args)
    end

    it 'includes the current timestamp' do
      expect(store).to include('t' => Time.now.to_f)
    end

    it 'does not include a unique job id' do
      expect(store).to_not have_key(described_class::UNIQUE_JOB_ID)
    end

    context 'when job options include params' do
      let(:additional_job_options) do
        { params: { a: 1, b: 2 } }
      end

      it 'includes params' do
        expect(store).to include(a: 1, b: 2)
      end
    end

    context 'when job options include unique' do
      let(:additional_job_options) { { unique: true } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end

      it 'generates a unique job id' do
        uuid = SecureRandom.uuid
        SecureRandom.stub(:uuid).and_return(uuid)
        expect(store[described_class::UNIQUE_JOB_ID]).to eq(uuid)
      end

      it 'maintains a stable id for this job instance' do
        expect(performable.store[described_class::UNIQUE_JOB_ID])
          .to eq(performable.store[described_class::UNIQUE_JOB_ID])
      end
    end

    context 'when job options include thottle' do
      let(:additional_job_options) { { throttle: true } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end

    context 'when job options include at' do
      let(:additional_job_options) { { at: 10.minutes.from_now } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end

    context 'when job options include in' do
      let(:additional_job_options) { { in: 1.minute } }

      it 'does not include the timestamp' do
        expect(store).to_not have_key('t')
      end
    end
  end

  describe '.perform' do
    subject(:perform) { perform_job(described_class, options) }

    context 'when job is not unique' do
      let(:additional_job_options) { { 't' => Time.now.to_f } }

      it 'executes the method' do
        DummyObject.should_receive(:do_something).with(*method_args).once
        perform
      end
    end

    context 'when job has unique identifier' do
      let(:additional_job_options) { { described_class::UNIQUE_JOB_ID => uuid } }

      let(:uuid) { ::SecureRandom.uuid }
      let(:other_uuid) { ::SecureRandom.uuid }

      context 'when there is a unique job id being tracked' do
        before do
          redis.hset(
            DelayedResque::DelayProxy::UNIQUE_JOBS_NAME,
            ::Resque.encode(base_job_options),
            tracked_uuid
          )
        end

        context 'when this job is the last unique job' do
          let(:tracked_uuid) { uuid }

          it 'executes the method' do
            DummyObject.should_receive(:do_something).with(*method_args).once
            perform
          end
        end

        context 'when this job is not the last unique job' do
          let(:tracked_uuid) { other_uuid }

          it 'does not execute the method' do
            DummyObject.should_not_receive(:do_something)
            perform
          end
        end
      end

      context 'when there is not a unique job id being tracked' do
        # We should never end up here in real life, but for the sake of completeness...
        # we can assume that *somehow* the unique job that was being tracked was already
        # processed and therefore this should be a no-op
        it 'does not execute the method' do
          DummyObject.should_not_receive(:do_something)
          perform
        end
      end
    end
  end
end
