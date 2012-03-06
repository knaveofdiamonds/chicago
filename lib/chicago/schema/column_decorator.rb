module Chicago
  module Schema
    class ColumnDecorator
      instance_methods.each do |m|
        undef_method m unless m =~ /(^__|^send$|^object_id$)/
      end

      # @private
      def initialize(column)
        @column = column
      end

      # @private
      def method_missing(*args, &block)
        @column.send(*args, &block)
      end
    end
  end
end
