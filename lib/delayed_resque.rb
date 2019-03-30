require 'resque'
require 'resque-scheduler'
require File.dirname(__FILE__) + '/delayed_resque/meta_data'
require File.dirname(__FILE__) + '/delayed_resque/message_sending'
require File.dirname(__FILE__) + '/delayed_resque/performable_method'

# Support delaying class methods.
Object.send(:include, DelayedResque::MessageSending)
