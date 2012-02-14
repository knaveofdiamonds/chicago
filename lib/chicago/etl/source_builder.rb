require 'chicago/etl/source'
require 'chicago/schema/dimension'

module Chicago
  module ETL
    class SourceBuilder
      # Builds a source named name, representing a table or query in db.
      def build(obj, db, &block)
        @options = {}
        instance_eval(&block) if block_given?
        _build(obj, db)
      end

      protected

      # Defines the columns in this source
      def columns(*column_names)
        @options[:columns] = column_names
      end

      # Override the table name of this source.
      #
      # By default, the table name will be the name of the source.
      def table_name(name)
        @options[:table_name] = name
      end

      private

      def _build(obj, db)
        case obj
        when Symbol
          build_from_name(obj, db)
        when Chicago::Schema::Dimension
          build_from_dimension(obj, db)
        end
      end
      
      def build_from_name(name, db)
        Source.new(name, db, @options)
      end

      def build_from_dimension(dimension, db)
        name = dimension.name.to_s.pluralize.to_sym
        set_columns_from_dimension(dimension)
        Source.new(name, db, @options)
      end

      def set_columns_from_dimension(dimension)
        @options[:columns] = dimension.columns.map(&:name)
        if @options[:columns].delete(:original_id)
          @options[:columns].unshift :id
        end
      end
    end
  end
end
