$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rspec'
require 'resque_scheduler'
require 'resque_spec'
require 'resque_spec/scheduler'
require 'delayed_resque'

Dir[File.expand_path(File.join(File.dirname(__FILE__),'support','**','*.rb'))].each {|f| require f}

root = File.expand_path(File.join(File.dirname(__FILE__), '..'))
ActiveRecord::Base.establish_connection(
	:adapter => "sqlite3",
	:database => "#{root}/test.db"
)

RSpec.configure do |config|
end