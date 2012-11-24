require 'mail'

module DelayedResque
  class PerformableMailer < PerformableMethod
    def self.perform(options)
      PerformableMethod.perform(options).deliver
    end
  end

  module DelayMail
    def delay(options = {})
      DelayProxy.new(PerformableMailer, self, options)
    end
  end
end

Mail::Message.class_eval do
  def delay(*args)
    raise RuntimeError, "Use MyMailer.delay.mailer_action(args) to delay sending of emails."
  end
end