module Arel # :nodoc: all
  module Nodes
    class Settings < Arel::Nodes::Unary
      def initialize(expr)
        raise ArgumentError.new("Settings must be a Hash") unless expr.is_a?(Hash)

        super
      end
    end
  end
end
