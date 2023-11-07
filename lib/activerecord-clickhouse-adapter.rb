# frozen_string_literal: true

require_relative "active_record/connection_adapters/clickhouse_adapter"

if defined?(Rails::Railtie)
  require "railtie"
  require "active_record/connection_adapters/clickhouse/tasks"
  ActiveRecord::Tasks::DatabaseTasks.register_task(/clickhouse/, "ActiveRecord::ConnectionAdapters::Clickhouse::Tasks")
end
