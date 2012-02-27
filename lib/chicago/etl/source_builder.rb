require 'chicago/etl/source'
require 'chicago/schema/dimension'

module Chicago
  module ETL
    class SourceBuilder
      # Builds a source named name, representing a table or query in db.
      def build(source_type, obj, options=nil, &block)
        @source_class = DatabaseSource
        @options = options || {}
        instance_eval(&block) if block_given?
        _build(obj)
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

      # Captures the block and passes it to the source as the dataset
      # option.
      #
      # The block is expected to be source-type specific, and used to
      # filter or otherwise modify the raw underlying data.
      def dataset(&block)
        @options[:dataset] = block
      end
      
      private

      def _build(obj)
        name = case obj
               when Symbol
                 obj
               when Chicago::Schema::Dimension
                 build_from_dimension(obj)
               when Chicago::Schema::Fact
                 build_from_fact(obj)
               end
        @source_class.new(name, @options)
      end
      
      def build_from_dimension(dimension)
        name = dimension.name.to_s.pluralize.to_sym
        set_columns_from_dimension(dimension)
        name
      end

      def build_from_fact(fact)
        set_columns_from_fact(fact)
        fact.name
      end

      def set_columns_from_dimension(dimension)
        @options[:columns] = dimension.columns.map(&:name)
        replace_original_id_with_id
      end

      def set_columns_from_fact(fact)
        @options[:columns] = fact.dimensions.map {|d| "#{d.name}_id".to_sym }
        @options[:columns] += fact.degenerate_dimensions.map(&:name) +
          fact.measures.map(&:name)
        replace_original_id_with_id
      end

      def replace_original_id_with_id
        if @options[:columns].delete(:original_id)
          @options[:columns].unshift :id
        end
      end
    end
  end
end
