module Chicago
  module ETL
    module Source
      class Database < Source
        attr_reader :dataset
        
        def initialize(name, options={})
          super
          @database = options[:db]
          @table_name = options[:table_name] || @name
          @dataset = (options[:dataset] || lambda {|ds| ds }).
            call(@database[table_name])
          @dataset = @dataset.select(*@columns) if @columns
          if options[:timestamps].nil?
            @timestamps = [:updated_at.qualify(@table_name), :created_at.qualify(@table_name)]
          else
            @timestamps = [options[:timestamps]].compact.flatten
          end
        end

        def columns
          super || @dataset.columns
        end

        # Returns the SQL query used to extract data from the source.
        #
        # By default returns all records; optionally an extract_from
        # date may be provided to extract records with modification
        # timestamps after this period of time. By default, the main
        # table's created_at and updated_at timestamps are considered.
        def query(extract_from=nil)
          dataset = (extract_from && ! @timestamps.empty?) ? 
            @dataset.filter(timestamp_filters(extract_from)) :
            @dataset
          dataset.sql
        end

        def staging_columns
          @dataset.columns
        end
        
        # Extracts data to the File, file_path.
        def extract(file_path, extract_from=nil)
          File.unlink(file_path) if File.exists?(file_path)
          # Makes this method thread-safe, at least as far as database
          # connections are concerned.
          @database.disconnect
          @database.run(query(extract_from) + " INTO OUTFILE '#{file_path}'")
        end
        
        # Returns true if the backing database table exists.
        #
        # Does not currently validate the column set.
        def valid?
          !! @database.table_exists?(table_name)
        end
        
        private

        attr_reader :table_name

        def timestamp_filters(extract_from)
          @timestamps.
            map {|timestamp| timestamp >= extract_from }.
            inject {|a,b| a | b }
        end
      end
    end
  end
end
