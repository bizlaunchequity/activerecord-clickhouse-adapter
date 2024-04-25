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
          im = ::Arel::InsertManager.new(arel_table)
          im.insert(arel_table[primary_key] => version.to_s, arel_table['active'] => 0)
          connection.insert(im, "#{self.class} Create Rollback Version", primary_key, version)
        end

        # def versions
        #   table = arel_table.dup
        #   table.final = true

        #   sm = ::Arel::SelectManager.new(table)
        #   sm.project(table[primary_key]).where(table[:active].eq(1))
        #   sm.order(table[primary_key].asc)

        #   connection.select_values(sm, "#{self.class} Load")
        # end

        def all_versions
          final.where(active: 1).order(:version).pluck(:version)
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
          sm = ::Arel::SelectManager.new(arel_table)
          sm.final! if connection.table_options(table_name)[:options] =~ /^ReplacingMergeTree/
          sm.project(::Arel.star)
          sm.where(arel_table[primary_key].eq(::Arel::Nodes::BindParam.new(key)))
          sm.order(arel_table[primary_key].asc)
          sm.limit = 1

          connection.select_one(sm, "#{self.class} Load")
        end

        def update_entry(key, new_value)
          create_entry(key, new_value)
        end
      end
    end
  end
end
