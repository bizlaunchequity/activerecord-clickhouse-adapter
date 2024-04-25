module CoreExtensions
  module ActiveRecord
    module Relation
      def settings(**opts)
        spawn.settings!(**opts)
      end

      def settings!(**opts)
        assert_mutability!
        check_command('SETTINGS')
        @values[:settings] = (@values[:settings] || {}).merge opts
        self
      end

      def final(final = true)
        spawn.final!
      end

      def final!
        assert_mutability!
        check_command('FINAL')
        @values[:final] = true
        self
      end

      def using(*opts)
        spawn.using!(*opts)
      end

      # @param [Array] opts
      def using!(*opts)
        assert_mutability!
        @values[:using] = opts
        self
      end

      private

      def check_command(cmd)
        raise ::ActiveRecord::ActiveRecordError.new("#{cmd} is a ClickHouse specific query clause") unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)
      end

      def build_arel(aliases = nil)
        arel = super

        arel.final! if @values[:final].present?
        arel.settings(@values[:settings]) if @values[:settings].present?
        arel.using(@values[:using]) if @values[:using].present?

        arel
      end
    end
  end
end
