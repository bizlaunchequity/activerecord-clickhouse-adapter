# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class SchemaMigration < ::ActiveRecord::SchemaMigration
        def create_table
          return if connection.table_exists?(table_name)

          version_options = connection.internal_string_options_for_primary_key
          table_options = {
            id: false, options: "ReplacingMergeTree(ver) ORDER BY (version)", if_not_exists: true
          }
          full_config = connection.full_config || {}

          if full_config[:distributed_service_tables]
            table_options.merge!(with_distributed: table_name, sharding_key: "cityHash64(version)")

            distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
          end

          connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
            t.string :version, **version_options
            t.column :active, "Int8", null: false, default: "1"
            t.datetime :ver, null: false, default: -> { "now()" }
          end
        end

        def delete_version(version)
          insert_manager = ::Arel::InsertManager.new
          insert_manager.insert([[arel_table[:version], version], [arel_table[:active], 0]])

          connection.do_execute(insert_manager.to_sql, "#{self.class} Destroy", format: nil)
        end

        def versions
          table = arel_table.dup
          table.final = true

          sm = ::Arel::SelectManager.new(table)
          sm.project(table[primary_key]).where(table[:active].eq(1))
          sm.order(table[primary_key].asc)

          connection.select_values(sm, "#{self.class} Load")
        end
      end

      class InternalMetadata < ::ActiveRecord::InternalMetadata
        def create_table
          return if connection.table_exists?(table_name)

          key_options = connection.internal_string_options_for_primary_key
          table_options = {
            id: false,
            options: "ReplacingMergeTree(created_at) PARTITION BY key ORDER BY key",
            if_not_exists: true
          }
          full_config = connection.full_config || {}

          if full_config[:distributed_service_tables]
            table_options.merge!(with_distributed: table_name, sharding_key: "cityHash64(created_at)")

            distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
          end

          connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
            t.string :key, **key_options
            t.string :value
            t.timestamps
          end
        end

        private

        def select_entry(key)
          table = arel_table.dup
          table.final = true

          sm = ::Arel::SelectManager.new(table)
          sm.project(::Arel::Nodes::SqlLiteral.new("*"))
          sm.where(table[primary_key].eq(::Arel::Nodes::BindParam.new(key)))
          sm.order(table[primary_key].asc)
          sm.limit = 1

          connection.select_all(sm, "#{self.class} Load").first
        end

        def update_entry(key, new_value)
          create_entry(key, new_value)
        end
      end
    end
  end
end
