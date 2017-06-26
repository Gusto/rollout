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
    with_feature(feature) do |f|
      f.percentage = 100
    end
  end

  def deactivate(feature)
    with_feature(feature) do |f|
      f.clear
    end
  end

  def delete(feature)
    feature_list.delete_feature(feature)
    @storage.del(key(feature))
  end

  def set(feature, desired_state)
    with_feature(feature) do |f|
      if desired_state
        f.percentage = 100
      else
        f.clear
      end
    end
  end

  def activate_group(feature, group)
    with_feature(feature) do |f|
      f.add_group(group)
    end
  end

  def deactivate_group(feature, group)
    with_feature(feature) do |f|
      f.remove_group(group)
    end
  end

  def activate_user(feature, user)
    with_feature(feature) do |f|
      f.add_user(user)
    end
  end

  def deactivate_user(feature, user)
    with_feature(feature) do |f|
      f.remove_user(user)
    end
  end

  def activate_users(feature, users)
    with_feature(feature) do |f|
      users.each{|user| f.add_user(user)}
    end
  end

  def deactivate_users(feature, users)
    with_feature(feature) do |f|
      users.each{|user| f.remove_user(user)}
    end
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
    string = @storage.get(key(feature))
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
    feature_keys = features.map{ |feature| key(feature) }
    @storage.mget(*feature_keys).map.with_index { |string, index| Feature.new(features[index], string, @options) }
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
      delete(feature)
    end

    feature_list.delete
  end

  private

  def key(name)
    "feature:#{name}"
  end

  def with_feature(feature)
    f = get(feature)
    yield(f)
    save(f)
  end

  def save(feature)
    @storage.set(key(feature.name), feature.serialize)
    feature_list.add_feature(feature.name)
  end

  def feature_list
    FeatureList.new(@storage)
  end

  class FeatureList
    FEATURES_KEY = "feature:__features__".freeze

    def initialize(storage)
      @storage = storage
    end

    def all
      @storage.smembers(FEATURES_KEY).map(&:to_sym)
    end

    def add_feature(feature)
      @storage.sadd(FEATURES_KEY, feature)
    end

    def delete_feature(feature)
      @storage.srem(FEATURES_KEY, feature)
    end

    def delete
      @storage.del(FEATURES_KEY)
    end
  end
end
