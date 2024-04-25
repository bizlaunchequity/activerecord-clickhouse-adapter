# frozen_string_literal: true

require "rails"

require "core_extensions/active_record/relation"
require 'core_extensions/arel/nodes/select_core'
require "core_extensions/arel/nodes/select_statement"
require "core_extensions/arel/select_manager"
require "core_extensions/arel/table"

class Railtie < Rails::Railtie
  initializer "clickhouse.load" do
    ActiveSupport.on_load :active_record do
      ActiveRecord::Relation.prepend(CoreExtensions::ActiveRecord::Relation)
      Arel::Nodes::SelectCore.prepend(CoreExtensions::Arel::Nodes::SelectCore)
      Arel::Nodes::SelectStatement.prepend(CoreExtensions::Arel::Nodes::SelectStatement)
      Arel::SelectManager.prepend(CoreExtensions::Arel::SelectManager)
      Arel::Table.prepend(CoreExtensions::Arel::Table)
    end
  end
end
