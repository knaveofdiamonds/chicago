module Chicago
  module ETL
    class ErrorHandler
      def initialize(db)
        @db = db
      end
      
      def error(error_details)
        @db[:etl_error_log].insert(error_details)
      end
    end
  end
end
