# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      module SchemaStatements
        DEFAULT_RESPONSE_FORMAT = 'JSONCompactEachRowWithNamesAndTypes'.freeze

        def execute(sql, name = nil, settings: {})
          do_execute(sql, name, settings: settings)
        end

        def exec_insert(sql, name, _binds, _pk = nil, _sequence_name = nil, returning: nil)
          new_sql = sql.dup.sub(/ (DEFAULT )?VALUES/, " VALUES")
          do_execute(new_sql, name, format: nil)
          true
        end

        def internal_exec_query(sql, name = nil, _binds = [], prepare: false, async: false)
          result = do_execute(sql, name)
          ActiveRecord::Result.new(result["meta"].map { |m| m["name"] }, result["data"], result["meta"].map { |m| [m["name"], type_map.lookup(m["type"])] }.to_h)
        rescue ActiveRecord::ActiveRecordError => e
          raise e
        rescue => e
          raise ActiveRecord::ActiveRecordError.new("Response: #{e.message}")
        end

        def exec_insert_all(sql, name)
          do_execute(sql, name, format: nil)
          true
        end

        def exec_update(sql, name = nil, _binds = [])
          do_execute(sql, name, format: nil)
          0
        end

        def exec_delete(sql, name = nil, _binds = [])
          log(sql, "#{adapter_name} #{name}") do
            res = request(sql)
            begin
              data = JSON.parse(res.header["x-clickhouse-summary"])
              data["result_rows"].to_i
            rescue JSONError
              0
            end
          end
        end

        def tables(name = nil)
          result = do_system_execute("SHOW TABLES WHERE name NOT LIKE '.inner_id.%'", name)
          return [] if result.nil?

          result["data"].flatten
        end

        def functions
          result = do_system_execute("SELECT name FROM system.functions WHERE origin = 'SQLUserDefined'")
          return [] if result.nil?
          result['data'].flatten
        end

        def show_create_function(function)
          do_execute("SELECT create_query FROM system.functions WHERE origin = 'SQLUserDefined' AND name = '#{function}'", format: nil)
        end

        def table_options(table)
          sql = show_create_table(table)
          { options: sql.gsub(/^(?:.*?)(?:ENGINE = (.*?))?( AS SELECT .*?)?$/, '\\1').presence, as: sql.match(/^CREATE (?:.*?) AS (SELECT .*?)$/).try(:[], 1) }.compact
        end

        # Not indexes on clickhouse
        def indexes(_table_name, _name = nil)
          []
        end

        def add_index_options(table_name, expression, **options)
          options.assert_valid_keys(:name, :type, :granularity, :first, :after)

          validate_index_length!(table_name, options[:name])

          IndexDefinition.new(table_name, options[:name], expression, options[:type], options[:granularity], first: options[:first], after: options[:after])
        end

        def add_index(table_name, expression, **options)
          index = add_index_options(apply_cluster(table_name), expression, **options)
          execute schema_creation.accept(CreateIndexDefinition.new(index))
        end

        def data_sources
          tables
        end

        def do_system_execute(sql, name = nil)
          log_with_debug(sql, "#{adapter_name} #{name}") do
            res = request(sql, DEFAULT_RESPONSE_FORMAT)
            process_response(res, DEFAULT_RESPONSE_FORMAT, sql)
          end
        end

        def do_execute(sql, name = nil, format: DEFAULT_RESPONSE_FORMAT, settings: {})
          log(sql, "#{adapter_name} #{name}") do
            res = request(sql, format, settings)
            process_response(res, format, sql)
          end
        end

        def assume_migrated_upto_version(version, _migrations_paths = nil)
          version = version.to_i
          sm_table = quote_table_name(schema_migration.table_name)

          migrated = migration_context.get_all_versions
          versions = migration_context.migrations.map(&:version)

          unless migrated.include?(version)
            exec_insert "INSERT INTO #{sm_table} (version) VALUES (#{quote(version.to_s)})", nil, nil
          end

          inserting = (versions - migrated).select { |v| v < version }
          return unless inserting.any?
          if (duplicate = inserting.detect { |v| inserting.count(v) > 1 })
            raise "Duplicate migration #{duplicate}. Please renumber your migrations to resolve the conflict."
          end

          do_execute(insert_versions_sql(inserting), nil, settings: { max_partitions_per_insert_block: [100, inserting.size].max })
        end

        def with_yaml_fallback(value) # :nodoc:
          if value.is_a?(Array)
            value
          else
            super
          end
        end

        private

        def request(sql, format = nil, settings = {})
          formatted_sql = apply_format(sql, format)
          request_params = @connection_config || {}
          @connection.post("/?#{request_params.merge(settings).to_param}", formatted_sql, 'User-Agent' => "Clickhouse ActiveRecord #{VERSION}")
        end

        def apply_format(sql, format)
          format ? "#{sql} FORMAT #{format}" : sql
        end

        def process_response(res, format, sql = nil)
          case res.code.to_i
          when 200
            raise ActiveRecord::ActiveRecordError, "Response code: #{res.code}:\n#{res.body}#{sql ? "\nQuery: #{sql}" : ''}" if res.body.to_s.include?("DB::Exception")

            format_body_response(res.body, format)
          else
            case res.body
            when /DB::Exception:.*\(UNKNOWN_DATABASE\)/
              raise ActiveRecord::NoDatabaseError
            when /DB::Exception:.*\(DATABASE_ALREADY_EXISTS\)/
              raise ActiveRecord::DatabaseAlreadyExists
            else
              raise ActiveRecord::ActiveRecordError.new("Response code: #{res.code}:\n#{res.body}")
            end
          end
        rescue JSON::ParserError
          res.body
        end

        def log_with_debug(sql, name = nil, &block)
          return yield unless @debug

          log(sql, "#{name} (system)", &block)
        end

        def schema_creation
          Clickhouse::SchemaCreation.new(self)
        end

        def create_table_definition(table_name, **)
          Clickhouse::TableDefinition.new(self, table_name, **)
        end

        def new_column_from_field(table_name, field, _definitions)
          sql_type = field[1]
          type_metadata = fetch_type_metadata(sql_type)
          default = field[3]
          default_value = extract_value_from_default(default)
          default_function = extract_default_function(default_value, default)
          ClickhouseColumn.new(field[0], default_value, type_metadata, field[1].include?("Nullable"), default_function)
        end

        protected

        def table_structure(table_name)
          @table_structure ||= {}
          @table_structure[table_name] ||= get_table_structure(table_name)
        end
        alias_method :column_definitions, :table_structure

        private

        def get_table_structure(table_name)
          result = do_system_execute("DESCRIBE TABLE `#{table_name}`", table_name)
          data = result["data"]

          return data unless data.empty?

          raise ActiveRecord::StatementInvalid.new("Could not find table '#{table_name}'")
        end

        # Extracts the value from a PostgreSQL column default definition.
        def extract_value_from_default(default)
          case default
            # Quoted types
          when /\Anow\(\)\z/m
            nil
            # Boolean types
          when "true", "false"
            default
            # Object identifier types
          when "''"
            ""
          when /\A-?\d+\z/
            ::Regexp.last_match(1)
          else
            # Anything else is blank, some user type, or some function
            # and we can't know the value of that, so return nil.
            nil
          end
        end

        def extract_default_function(default_value, default) # :nodoc:
          default if has_default_function?(default_value, default)
        end

        def has_default_function?(default_value, default) # :nodoc:
          !default_value && (/\w+\(.*\)/ === default)
        end

        def format_body_response(body, format)
          return body if body.blank?

          case format
          when 'JSONCompact'
            format_from_json_compact(body)
          when 'JSONCompactEachRowWithNamesAndTypes'
            format_from_json_compact_each_row_with_names_and_types(body)
          else
            body
          end
        end

        def format_from_json_compact(body)
          parse_json_payload(body)
        end

        def format_from_json_compact_each_row_with_names_and_types(body)
          rows = body.split("\n").map { |row| parse_json_payload(row) }
          names, types, *data = rows

          meta = names.zip(types).map do |name, type|
            {
              'name' => name,
              'type' => type
            }
          end

          {
            'meta' => meta,
            'data' => data
          }
        end

        def parse_json_payload(payload)
          JSON.parse(payload, decimal_class: BigDecimal)
        end
      end
    end
  end
end
