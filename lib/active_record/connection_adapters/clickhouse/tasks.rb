# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Clickhouse
      class Tasks
        attr_reader :configuration

        def initialize(configuration)
          @configuration = configuration.with_indifferent_access
        end

        def create(connection_already_established = false)
          establish_connection unless connection_already_established
          connection.create_database configuration[:database]
        rescue ActiveRecord::StatementInvalid => e
          raise ActiveRecord::DatabaseAlreadyExists if e.cause.to_s.include?("already exists")

          raise
        rescue ActiveRecord::ActiveRecordError => e
          raise ActiveRecord::DatabaseAlreadyExists if e.message.include?("already exists")

          raise
        end

        def drop
          establish_connection
          connection.drop_database(configuration[:database])
        end

        def purge
          ActiveRecord::Base.connection_handler.clear_active_connections!(:all)
          drop
          create(true)
        end

        private

        def connection
          ActiveRecord::Base.connection
        end

        def establish_connection
          ActiveRecord::Base.establish_connection(configuration)
        end
      end
    end
  end
end
