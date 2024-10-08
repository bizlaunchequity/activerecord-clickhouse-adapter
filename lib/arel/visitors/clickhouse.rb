require "arel/visitors/to_sql"

module Arel
  module Visitors
    class Clickhouse < ::Arel::Visitors::ToSql
      def aggregate(name, o, collector)
        # replacing function name for materialized view
        if o.expressions.first && o.expressions.first != '*' && !o.expressions.first.is_a?(String) && o.expressions.first.relation&.is_view
          super("#{name.downcase}Merge", o, collector)
        else
          super
        end
      end

      def visit_Arel_Table(o, collector)
        collector = super
        collector << ' FINAL ' if o.final
        collector
      end

      def visit_Arel_Nodes_SelectOptions(o, collector)
        maybe_visit o.settings, super
      end

      def visit_Arel_Nodes_Settings(o, collector)
        return collector if o.expr.empty?

        collector << "SETTINGS "
        o.expr.each_with_index do |(key, value), i|
          collector << ", " if i > 0
          collector << key.to_s.gsub(/\W+/, "")
          collector << " = "
          collector << sanitize_as_setting_value(value)
        end
        collector
      end

      def visit_Arel_Nodes_Matches(o, collector)
        op = o.case_sensitive ? " LIKE " : " ILIKE "
        collector = infix_value o, collector, op

        # collector = visit o.left, collector
        # collector << " ILIKE "
        # collector = visit o.right, collector
      end

      def visit_Arel_Nodes_DoesNotMatch(o, collector)
        op = o.case_sensitive ? " NOT LIKE " : " NOT ILIKE "
        collector = infix_value o, collector, op
      end

      def sanitize_as_setting_value(value)
        if value == :default
          "DEFAULT"
        else
          quote(value)
        end
      end

      def sanitize_as_setting_name(value)
        return value if Arel::Nodes::SqlLiteral === value
        @connection.sanitize_as_setting_name(value)
      end
    end
  end
end
