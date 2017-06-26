require "rollout/version"
require "rollout/feature"
require "zlib"
require "set"
require "json"

class Rollout
  # TODO (v3):  Deprecate this public interface
  RAND_BASE = ::Feature::RAND_BASE

  def initialize(redis, opts = {})
    @redis = redis
    @options = opts
    @groups  = { all: lambda { |user| true } }
  end

  def activate(feature)
    activate_percentage(feature, 100)
  end

  def deactivate(feature)
    feature_storage.deactivate_feature(feature)
  end

  def delete(feature)
    feature_storage.delete_feature(feature)
  end

  def set(feature, desired_state)
    if desired_state
      activate_percentage(feature, 100)
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
    feature_storage.set_percentage(feature, percentage)
  end

  def deactivate_percentage(feature)
    activate_percentage(feature, 0)
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
    feature_storage.set_feature_data(feature, data)
  end

  def clear_feature_data(feature)
    feature_storage.clear_feature_data(feature)
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

  def feature_storage
    @feature_storage ||= FeatureStorage.new(@redis)
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

    def activate_group(feature, group)
      @redis.multi do
        @redis.sadd(key(feature, KEY_GROUPS), group)
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def deactivate_group(feature, group)
      @redis.multi do
        @redis.srem(key(feature, KEY_GROUPS), group)
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def activate_users(feature, users)
      @redis.multi do
        @redis.sadd(key(feature, KEY_USERS), users)
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def deactivate_users(feature, users)
      @redis.multi do
        @redis.srem(key(feature, KEY_USERS), users)
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def delete_feature(feature)
      @redis.multi do
        @redis.srem(FEATURES_KEY, feature)
        @redis.del(SUBKEYS.map { |subkey| key(feature, subkey) })
      end
    end

    def deactivate_feature(feature)
      @redis.multi do
        @redis.del(SUBKEYS.map { |subkey| key(feature, subkey) })
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def get_feature_data(feature)
      raw_data = @redis.get(key(feature, KEY_DATA))
      raw_data.nil? || raw_data.strip.empty? ? {} : JSON.parse(raw_data)
    end

    def set_feature_data(feature, data)
      if data.is_a? Hash
        old_data = get_feature_data(feature)
        @redis.multi do
          @redis.set(key(feature, KEY_DATA), old_data.merge!(data).to_json)
          @redis.sadd(FEATURES_KEY, feature)
        end
      end
    end

    def clear_feature_data(feature)
      @redis.multi do
        @redis.set(key(feature, KEY_DATA), "{}")
        @redis.sadd(FEATURES_KEY, feature)
      end
    end

    def delete
      @redis.del(FEATURES_KEY)
    end

    def clear!
      redis_keys = [FEATURES_KEY] + all.flat_map do |feature|
        SUBKEYS.map { |subkey| key(feature, subkey) }
      end
      @redis.del(redis_keys)
    end

    def key(name, type)
      "feature:#{name}:#{type}"
    end
  end
end
