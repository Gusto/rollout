require "rollout/version"
require "rollout/feature"
require "zlib"
require "set"
require "json"

class Rollout
  def initialize(redis, opts = {})
    @redis = redis
    @options = opts
    @groups  = { all: lambda { |user| true } }
  end

  def activate(feature)
    activate_percentage(feature, 100)
  end

  def deactivate(feature)
    get(feature).clear!
  end

  def delete(feature)
    get(feature).clear!
    feature_list.delete_feature(feature)
  end

  def set(feature, desired_state)
    if desired_state
      activate_percentage(feature, 100)
    else
      deactivate(feature)
    end
  end

  def activate_group(feature, group)
    get(feature).add_group(group)
  end

  def deactivate_group(feature, group)
    get(feature).remove_group(group)
  end

  def activate_user(feature, user)
    activate_users(feature, [user])
  end

  def deactivate_user(feature, user)
    deactivate_users(feature, [user])
  end

  def activate_users(feature, users)
    get(feature).add_users(users)
  end

  def deactivate_users(feature, users)
    get(feature).remove_users(users)
  end

  def define_group(group, &block)
    @groups[group.to_sym] = block
  end

  def active?(feature, user = nil)
    get(feature).active?(self, user)
  end

  def user_in_active_users?(feature, user = nil)
    get(feature).user_in_active_users?(user)
  end

  def inactive?(feature, user = nil)
    !active?(feature, user)
  end

  def activate_percentage(feature, percentage)
    get(feature).percentage = percentage
  end

  def deactivate_percentage(feature)
    activate_percentage(feature, 0)
  end

  def active_in_group?(group, user)
    f = @groups[group.to_sym]
    f && f.call(user)
  end

  def get(feature)
    feature_cache[feature]
  end

  def set_feature_data(feature, data)
    get(feature).data = data
  end

  def clear_feature_data(feature)
    get(feature).clear_data!
  end

  def multi_get(*features)
    features.map do |feature|
      get(feature)
    end
  end

  def features
    feature_list.all
  end

  def feature_states(user = nil)
    features.each_with_object({}) do |f, hash|
      hash[f] = active?(f, user)
    end
  end

  def active_features(user = nil)
    features.select do |f|
      active?(f, user)
    end
  end

  def clear!
    features.each do |feature|
      get(feature).clear!
    end
    feature_list.clear!
  end

  private

  def get_uncached(feature)
    feature_list.add_feature(feature)
    Feature.new(feature, @redis, @options)
  end

  # We can cache aggresively because the feature class re-fetches things correctly
  def feature_cache
    @feature_cache ||= Hash.new {|h, key| h[key] = get_uncached(key) }
  end

  def feature_list
    @feature_list ||= FeatureList.new(@redis)
  end

  class FeatureList
    FEATURES_KEY   = "feature:__features__".freeze

    def initialize(redis)
      @redis = redis
    end

    def all
      @redis.smembers(FEATURES_KEY).map(&:to_sym)
    end

    def add_feature(feature)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def delete_feature(feature)
      @redis.srem(FEATURES_KEY, feature)
    end

    def clear!
      @redis.del(FEATURES_KEY)
    end
  end
end
