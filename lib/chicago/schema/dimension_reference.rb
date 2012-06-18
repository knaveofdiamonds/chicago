require 'chicago/schema/column'
require 'forwardable'

module Chicago
  module Schema
    # A reference to a dimension - supports the API of Column and of
    # Dimension, so you can treat it as either.
    class DimensionReference < Column
      extend Forwardable

      def_delegators :@dimension, :columns, :column_definitions, :identifiers, :main_identifier, :identifiable?, :original_key, :natural_key, :table_name, :[], :key_table_name

      attr_reader :key_name
      
      def initialize(name, dimension, opts={})
        super name, :integer, opts.merge(:min => 0)
        @dimension = dimension
        @table_name = "dimension_#{@name}".to_sym
        @key_name   = "#{@name}_dimension_id".to_sym
      end

      def to_hash
        hsh = super
        hsh[:name] = @key_name
        hsh
      end
      
      def qualify(col)
        col.qualify_by(@table_name)
      end

      def qualify_by(table)
        @key_name.qualify(table)
      end

      # @private
      def kind_of?(klass)
        klass == Chicago::Schema::Dimension || super
      end
      
      # Dimension references are visitable
      def visit(visitor)
        visitor.visit_dimension_reference(self)
      end
    end
  end
end
