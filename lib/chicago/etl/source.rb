module Chicago
  module ETL
    class Source
      # The name of this data source. Must be unique across an ETL
      # context.
      attr_reader :name
      
      def initialize(name, options={})
        @name = name.to_sym
        @columns = options[:columns]
      end

      # The columns defined in this data source.
      def columns
        @columns
      end

      # Returns true if this source is valid.
      #
      # May be overridden by subclasses; returns true by default.
      def valid?
        true
      end
    end

    class DatabaseSource < Source
      attr_reader :dataset

      def initialize(name, options={})
        super
        @database = options[:db]
        @table_name = options[:table_name] || @name
        @dataset = (options[:dataset] || lambda {|ds| ds }).
          call(@database[table_name])
        @dataset = @dataset.select(*@columns) if @columns
      end

      def columns
        super || @dataset.columns
      end
      
      # Returns true if the backing database table exists.
      #
      # Does not currently validate the column set.
      def valid?
        !! @database.table_exists?(table_name)
      end
            
      private

      attr_reader :table_name
    end
  end
end
