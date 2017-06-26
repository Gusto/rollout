require "rollout/version"
require "rollout/feature"
require "zlib"
require "set"
require "json"

class Rollout
  # TODO (v3):  Deprecate this public interface
  RAND_BASE = ::Feature::RAND_BASE

  def initialize(storage, opts = {})
    @storage = storage
    @options = opts
    @groups  = { all: lambda { |user| true } }
  end

  def activate(feature)
    feature_storage.set_percentage(feature, 100)
  end

  def deactivate(feature)
    with_feature(feature) do |f|
      f.clear
    end
  end

  def delete(feature)
    feature_storage.delete_feature(feature)
  end

  def set(feature, desired_state)
    if desired_state
      feature_storage.set_percentage(feature, 100)
    else
      deactivate(feature)
    end
  end

  def activate_group(feature, group)
    feature_storage.activate_group(feature, group)
  end

  def deactivate_group(feature, group)
    feature_storage.deactivate_group(feature, group)
  end

  def activate_user(feature, user)
    activate_users(feature, [user])
  end

  def deactivate_user(feature, user)
    deactivate_users(feature, [user])
  end

  def activate_users(feature, users)
    feature_storage.activate_users(feature, users.map { |u| user_id_for_user(u) })
  end

  def deactivate_users(feature, users)
    feature_storage.deactivate_users(feature, users.map { |u| user_id_for_user(u) })
  end

  def define_group(group, &block)
    @groups[group.to_sym] = block
  end

  def active?(feature, user = nil)
    feature = get(feature)
    feature.active?(self, user)
  end

  def user_in_active_users?(feature, user = nil)
    feature = get(feature)
    feature.user_in_active_users?(user)
  end

  def inactive?(feature, user = nil)
    !active?(feature, user)
  end

  def activate_percentage(feature, percentage)
    with_feature(feature) do |f|
      f.percentage = percentage
    end
  end

  def deactivate_percentage(feature)
    with_feature(feature) do |f|
      f.percentage = 0
    end
  end

  def active_in_group?(group, user)
    f = @groups[group.to_sym]
    f && f.call(user)
  end

  def get(feature)
    string = feature_storage.fetch_feature(feature)
    Feature.new(feature, string, @options)
  end

  def set_feature_data(feature, data)
    with_feature(feature) do |f|
      f.data.merge!(data) if data.is_a? Hash
    end
  end

  def clear_feature_data(feature)
    with_feature(feature) do |f|
      f.data = {}
    end
  end

  def multi_get(*features)
    feature_storage.fetch_multi_features(features).map do |name, string|
      Feature.new(name, string, @options)
    end
  end

  def features
    feature_storage.all
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
    feature_storage.clear!
  end

  private

  def with_feature(feature)
    f = get(feature)
    yield(f)
    feature_storage.save_feature(f)
  end

  def feature_storage
    @feature_storage ||= FeatureStorage.new(@storage)
  end

  def user_id_for_user(user)
    if user.is_a?(Integer) || user.is_a?(String)
      user.to_s
    else
      user.send(id_user_by).to_s
    end
  end

  def id_user_by
    @options[:id_user_by] || :id
  end

  class FeatureStorage
    FEATURES_KEY   = "feature:__features__".freeze
    KEY_PERCENTAGE = "percentage".freeze
    KEY_USERS      = "users".freeze
    KEY_GROUPS     = "groups".freeze
    KEY_DATA       = "data".freeze
    SUBKEYS        = [KEY_PERCENTAGE, KEY_USERS, KEY_GROUPS, KEY_DATA].freeze

    def initialize(redis)
      @redis = redis
    end

    def all
      @redis.smembers(FEATURES_KEY).map(&:to_sym)
    end

    def set_percentage(feature, percentage)
      @redis.set(key(feature, KEY_PERCENTAGE), percentage)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def fetch_feature(feature)
      percentage, users, groups, data = @redis.multi do
        @redis.get(key(feature, KEY_PERCENTAGE))
        @redis.smembers(key(feature, KEY_USERS))
        @redis.smembers(key(feature, KEY_GROUPS))
        @redis.get(key(feature, KEY_DATA))
      end
      "#{percentage}|#{users.join(',')}|#{groups.join(',')}|#{data}"
    end

    def fetch_multi_features(features)
      features.map { |feature| [feature, fetch_feature(feature)] }
    end

    def save_feature(feature)
      @redis.multi do
        serialized = feature.serialize.split('|', 4)
        @redis.set(key(feature.name, KEY_PERCENTAGE), feature.percentage)

        # TODO: we clear lists before we set new values, this should be refactored out later
        @redis.del(key(feature.name, KEY_USERS))
        @redis.sadd(key(feature.name, KEY_USERS), feature.users.to_a) if feature.users.size > 0
        @redis.del(key(feature.name, KEY_GROUPS))
        @redis.sadd(key(feature.name, KEY_GROUPS), feature.groups.to_a) if feature.groups.size > 0

        @redis.set(key(feature.name, KEY_DATA), serialized[3])
        @redis.sadd(FEATURES_KEY, feature.name)
      end
    end

    def activate_group(feature, group)
      @redis.sadd(key(feature, KEY_GROUPS), group)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def deactivate_group(feature, group)
      @redis.srem(key(feature, KEY_GROUPS), group)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def activate_users(feature, users)
      @redis.sadd(key(feature, KEY_USERS), users)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def deactivate_users(feature, users)
      @redis.srem(key(feature, KEY_USERS), users)
      @redis.sadd(FEATURES_KEY, feature)
    end

    def delete_feature(feature)
      @redis.multi do
        @redis.srem(FEATURES_KEY, feature)
        SUBKEYS.each do |subkey|
          @redis.del(key(feature, subkey))
        end
      end
    end

    def delete
      @redis.del(FEATURES_KEY)
    end

    def clear!
      all.each do |feature|
        delete_feature(feature)
      end
      delete
    end

    def key(name, type)
      "feature:#{name}:#{type}"
    end
  end
end
