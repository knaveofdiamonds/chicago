module Chicago
  module ETL
    class Source
      # The name of this data source. Must be unique across an ETL
      # context.
      attr_reader :name

      # The columns defined in this data source.
      attr_reader :columns

      def initialize(name, db, options={})
        @name = name.to_sym
        @db = db
        @columns = options[:columns] || []
        @table_name = options[:table_name] || @name
      end

      # Returns true if the backing database table exists.
      def valid?
        !! @db.table_exists?(@table_name)
      end
    end
  end
end
