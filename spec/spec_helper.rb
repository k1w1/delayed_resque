$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rspec'
require 'resque_scheduler'
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

RSpec.configure do |config|
  config.include ActiveSupport::Testing::TimeHelpers
end
