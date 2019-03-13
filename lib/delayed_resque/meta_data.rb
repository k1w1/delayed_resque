module DelayedResque
  class MetaData
    def self.store_meta_data(klass, args, meta = {})
      Resque.redis.set(key(klass, args), Resque.encode(meta))
    end

    def self.load_meta_data(klass, args)
      val = Resque.decode(Resque.redis.get(key(klass, args)))
      Rails.logger.debug("Loaded data #{key(klass, args).inspect} #{val.inspect}")
      val
    end

    def self.delete_meta_data(klass, args)
      Resque.redis.del(key(klass, args))
    end

    def self.key(klass, args)
      "meta:#{Resque.encode('class' => klass.to_s, 'args' => args)}"
    end
  end
end