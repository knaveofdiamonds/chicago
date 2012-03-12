require 'fileutils'

module Chicago
  module ETL
    module Source
      class Csv < Source
        def initialize(name, options={})
          super
          @source_path = options[:source_path]
        end

        # Copies the source CSV to the destination expected when
        # staging, if the file exists.
        #
        # extract_from is included here for interface compatibility,
        # but has no effect.
        def extract(file_path, extract_from=nil)
          FileUtils.mv(@source_path, file_path) if File.exists?(@source_path)
        end

        def load_data_options
          "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES"
        end

        # Returns true if a source file path has been set.
        def valid?
          ! @source_path.nil?
        end
      end
    end
  end
end
