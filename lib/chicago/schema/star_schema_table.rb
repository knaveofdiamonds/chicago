module Chicago
  module Schema
    # Base class for both Dimensions and Facts.
    class StarSchemaTable
      # Returns the name of this dimension
      attr_reader :name

      # Returns or sets the database table name for this dimension.
      # By default, <name>_dimension.
      attr_accessor :table_name

      class << self
        # Creates a new dimension or fact named +name+
        #
        # This should be called on subclasses - i.e. Dimension.define or
        # Fact.define, not this class.
        def define(name, opts={}, &block)
          definition = self.new(name, opts)
          definition.instance_eval(&block) if block_given?
          @definitions ||= {}
          @definitions[definition.name] = definition
        end

        # Removes all previously defined Facts or Dimensions from the list
        # of known definitions.
        def clear_definitions
          @definitions = {}
        end

        # Returns a list of all defined Facts or Dimensions.
        def definitions
          (@definitions || {}).values
        end
      end

      # Returns a schema hash for use by Sequel::MigrationBuilder,
      # defining all the RDBMS tables needed to store and build this 
      # table.
      #
      # Overriden by subclasses.
      def db_schema(type_converter) ; end

      protected

      def initialize(name, opts={})
        @name = name.to_sym
      end

      # Returns the standard index name for a column / dimension name.
      def index_name(name)
        "#{name}_idx".to_sym
      end

      # Returns a hash defining the main table for the dimension or fact.
      def base_table(type_converter)
        {
          :primary_key => :id,
          :table_options => type_converter.table_options,
          :indexes => indexes,
          :columns => [{:name => :id, :column_type => :integer, :unsigned => true}] + column_definitions.map {|c| c.db_schema(type_converter) }
        }
      end
    end
  end
end
