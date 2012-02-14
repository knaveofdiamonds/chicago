module Chicago
  module ETL
    class Source
      # The name of this data source. Must be unique across an ETL
      # context.
      attr_reader :name

      def initialize(name, options={})
        @name = name.to_sym
        @db = options[:db]
        @columns = options[:columns]
        @table_name = options[:table_name] || @name
      end

      # The columns defined in this data source.
      def columns
        @columns || @db[table_name].columns
      end
      
      # Returns true if the backing database table exists.
      #
      # Does not currently validate the column set.
      def valid?
        !! @db.table_exists?(table_name)
      end

      private

      attr_reader :table_name
    end
  end
end
