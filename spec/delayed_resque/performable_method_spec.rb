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

  describe "#store" do

  end

  describe ".perform" do
    subject(:perform) do
      perform_job(described_class, base_job_options.merge(additional_job_options))
    end

    let(:object) { 'CLASS:DummyObject' }
    let(:method) { 'do_something' }
    let(:method_args) { [123] }
    let(:base_job_options) do
      {
        "obj" => object,
        "method" => method,
        "args" => method_args
      }
    end
    let(:additional_job_options) { {} }

    context "when job is not unique" do
      let(:additional_job_options) do
        { "t" => Time.now.to_f }
      end

      it "executes the method" do
        DummyObject.should_receive(:do_something).with(*method_args).once
        perform
      end
    end

    context "when job has unique identifier" do
      let(:additional_job_options) do
        { "job_uuid" => uuid }
      end

      let(:uuid) { ::SecureRandom.uuid }
      let(:other_uuid) { ::SecureRandom.uuid }

      context "when there is a unique job id being tracked" do
        before do
          redis.hset(
            DelayedResque::DelayProxy::UNIQUE_JOBS_NAME,
            ::Resque.encode(base_job_options),
            tracked_uuid
          )
        end

        context "when this job is the last unique job" do
          let(:tracked_uuid) { uuid }

          it "executes the method" do
            DummyObject.should_receive(:do_something).with(*method_args).once
            perform
          end
        end

        context "when this job is not the last unique job" do
          let(:tracked_uuid) { other_uuid }

          it "does not execute the method" do
            DummyObject.should_not_receive(:do_something)
            perform
          end
        end
      end

      context "when there is not a unique job id being tracked" do
        # We should never end up here in real life, but for the sake of completeness...
        # we can assume that *somehow* the unique job that was being tracked was already
        # processed and therefore this should be a no-op
        it "does not execute the method" do
          DummyObject.should_not_receive(:do_something)
          perform
        end
      end
    end
  end
end
