module Chicago
  module ETL
    ERROR_SEVERITIES = {
      :invalid_field_value => 2,
      :missing_field_value => 2
    }
    
    class DimensionScreen
      attr_reader :name, :version
      
      def initialize(dimension, error_record)
        @version = 1
        @name = self.class.name
        @dimension = dimension
        @error_record = error_record
      end

      def screen(row)
        @dimension.
          columns.
          product([:missing_value_check, :invalid_value_check]).
          each do |(column, check)|
          send(check, row, column)
        end
        row
      end

      def missing_value_check(row, column)
        if ! column.null? && row[column.name].nil?
          field_error row, column, :missing_field_value
          true
        end
      end

      def invalid_value_check(row, column)
        if column.elements && ! column.elements.include?(row[column.name])
          field_error row, column, :invalid_field_value, :error_detail => row[column.name]
          row[column.name] = column.default
          true
        end
      end
      
      def field_error(row, column, type, opts={})
        @error_record.error({:process_name  => @name,
                              :process_version => @version,
                              :table => @dimension.table_name,
                              :field => column.name,
                              :field_id => row[:id],
                              :error => type,
                              :severity => ERROR_SEVERITIES[type]
                            }.merge(opts))
      end
    end
  end
end
