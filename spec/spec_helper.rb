$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rspec'
require 'mock_redis'
require 'resque-scheduler'
require 'resque_spec'
require 'resque_spec/scheduler'
require 'active_support/testing/time_helpers'
require 'delayed_resque'

Rails.logger = Logger.new(STDOUT)
Rails.logger.level = Logger::ERROR

Dir[File.expand_path(File.join(File.dirname(__FILE__),'support','**','*.rb'))].each {|f| require f}

root = File.expand_path(File.join(File.dirname(__FILE__), '..'))
ActiveRecord::Base.establish_connection(
	:adapter => "sqlite3",
	:database => "#{root}/test.db"
)

# Based on https://git.io/Js25d
module PerformJob
  # Perform job inline, firing any resque hooks
  def perform_job(klass, *args)
    resque_job = Resque::Job.new(:testqueue, 'class' => klass, 'args' => args)
    resque_job.perform
  end
end

RSpec.configure do |config|
  config.include ActiveSupport::Testing::TimeHelpers
  config.include PerformJob

  config.before do
    Resque.redis = MockRedis.new
    ResqueSpec.reset!
  end
end
