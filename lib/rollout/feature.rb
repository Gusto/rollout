class Feature
  RAND_BASE = (2**32 - 1) / 100.0

  ATTR_KEY_PERCENTAGE = "percentage".freeze
  ATTR_KEY_USERS      = "users".freeze
  ATTR_KEY_GROUPS     = "groups".freeze
  ATTR_KEY_DATA       = "data".freeze
  SUBKEYS             = [ ATTR_KEY_PERCENTAGE,
                          ATTR_KEY_USERS,
                          ATTR_KEY_GROUPS,
                          ATTR_KEY_DATA ].freeze

  attr_reader :name, :options

  def initialize(name, redis, opts = {})
    @options = opts
    @name    = name
    @redis = redis
  end

  def percentage
    (@redis.get(key(ATTR_KEY_PERCENTAGE)) || 0).to_f
  end

  def percentage=(new_percentage)
    @redis.set(key(ATTR_KEY_PERCENTAGE), new_percentage)
  end

  def users
    users = @redis.smembers(key(ATTR_KEY_USERS))
    if @options[:use_sets]
      users.to_set
    else
      users.sort
    end
  end

  def add_users(users)
    @redis.sadd(key(ATTR_KEY_USERS), users.map { |u| user_id(u) })
  end

  def remove_users(users)
    @redis.srem(key(ATTR_KEY_USERS), users.map { |u| user_id(u) })
  end

  def groups
    groups = @redis.smembers(key(ATTR_KEY_GROUPS)).map(&:to_sym)
    if @options[:use_sets]
      groups.to_set
    else
      groups.sort
    end
  end

  def add_group(group)
    @redis.sadd(key(ATTR_KEY_GROUPS), group)
  end

  def remove_group(group)
    @redis.srem(key(ATTR_KEY_GROUPS), group)
  end

  def data
    raw_data = @redis.get(key(ATTR_KEY_DATA))
    raw_data.nil? || raw_data.strip.empty? ? {} : JSON.parse(raw_data)
  end

  def data=(new_data)
    if new_data.is_a? Hash
      old_data = data
      @redis.set(key(ATTR_KEY_DATA), old_data.merge!(new_data).to_json)
    end
  end

  def clear_data!
    @redis.set(key(ATTR_KEY_DATA), "{}")
  end

  def active?(rollout, user)
    if percentage == 100 # Short circuit this case so we don't start looking up users
      true
    elsif user
      id = user_id(user)
      user_in_percentage?(id) || user_in_active_users?(id) || user_in_active_group?(user, rollout)
    else
      false
    end
  end

  def user_in_active_users?(user)
    @redis.sismember(key(ATTR_KEY_USERS), user)
  end

  def to_hash
    {
      percentage: percentage,
      groups: groups,
      users: users
    }
  end

  def clear!
    @redis.del(SUBKEYS.map { |subkey| key(subkey) })
  end

  private
    def key(type)
      "feature:#{name}:#{type}"
    end

    def user_id(user)
      if user.is_a?(Integer) || user.is_a?(String)
        user.to_s
      else
        user.send(id_user_by).to_s
      end
    end

    def id_user_by
      @options[:id_user_by] || :id
    end

    def user_in_percentage?(user)
      Zlib.crc32(user_id_for_percentage(user)) < RAND_BASE * percentage
    end

    def user_id_for_percentage(user)
      if @options[:randomize_percentage]
        user_id(user).to_s + @name.to_s
      else
        user_id(user)
      end
    end

    def user_in_active_group?(user, rollout)
      groups.any? do |g|
        rollout.active_in_group?(g, user)
      end
    end
end
