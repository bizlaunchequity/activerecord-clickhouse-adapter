module CoreExtensions
  module ActiveRecord
    module Relation
      def settings(**opts)
        check_command("SETTINGS")
        @values[:settings] = (@values[:settings] || {}).merge opts
        self
      end

      def final(final = true)
        check_command("FINAL")
        @table = @table.dup
        @table.final = final
        self
      end

      private

      def check_command(cmd)
        raise ::ActiveRecord::ActiveRecordError.new("#{cmd} is a ClickHouse specific query clause") unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(aliases = nil)
        arel = super

        arel.settings(@values[:settings]) if @values[:settings].present?

        arel
      end
    end
  end
end
