module Chicago
  module ETL
    module Source
      class Source
        class << self
          def available_sources
            @available_sources ||= {}
          end

          def inherited(klass)
            key = klass.name.demodulize.underscore.to_sym
            available_sources[key] = klass
          end
        end

        # The name of this data source. Must be unique across an ETL
        # context.
        attr_reader :name

        # The name of the table in the staging database where source
        # data should be extracted.
        attr_reader :staging_table

        def initialize(name, options={})
          @name = name.to_sym
          @staging_table = options[:staging_table] || "original_#{@name}".to_sym
          @columns = options[:columns]
        end

        # The columns defined in this data source.
        def columns
          @columns
        end

        # Extracts data to the File-like object at file_path.
        #
        # By default, does nothing. Should be overridden by subclasses. 
        def extract(file_path, extract_from=nil)
        end

        def staging_columns
          columns
        end
        
        # Stages the data from this source into a staging table for
        # further processing.
        def stage(db, file_path)
          sql = ["LOAD DATA INFILE '#{file_path}' REPLACE INTO TABLE `#{staging_table}`", load_data_options, "(`#{staging_columns.join('`,`')}`)"].compact.join(' ')
          db.run(sql)
        end

        # Returns true if this source is valid.
        #
        # May be overridden by subclasses; returns true by default.
        def valid?
          true
        end

        protected

        # Specifies formatting options to be passed to LOAD DATA when
        # staging this dataset
        def load_data_options
        end
      end
    end
  end
end
