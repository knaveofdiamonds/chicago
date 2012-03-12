module Chicago
  module ETL
    class Extraction
      attr_reader :source

      def initialize(source, staging_db)
        @source = source
        @db = staging_db
      end

      def run(etl_batch, extract_from=nil)
        perform_task(etl_batch) do
          path = File.join(etl_batch.dir, source.name.to_s)
          extract_from ||= @db[:etl_extraction_times].
            where(:name => source.name.to_s).get(:extract_from)
          source.extract(path, extract_from)
          source.stage(@db, path) if File.exists?(path)
        end
      end

      private
      
      def perform_task(etl_batch)
        etl_batch.perform_task(:extract, source.name) do
          etl_batch.log.debug("Starting extracting #{source.name}")
          new_extract_from = Time.now
          # Disconnect in case we fork or otherwise multithread when
          # performing the extraction.
          @db.disconnect
          yield
          update_extract_from(new_extract_from)
          etl_batch.log.debug("Finishing extracting #{source.name}")
        end
      end

      def update_extract_from(new_extract_from)
        ds = @db[:etl_extraction_times].where(:name => source.name.to_s)
        if ds.get(:id)
          ds.update(:extract_from => new_extract_from)
        else
          @db[:etl_extraction_times].insert(:name => source.name.to_s, :extract_from => new_extract_from)
        end
      end
    end
  end
end
