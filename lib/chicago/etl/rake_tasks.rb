require 'rake/tasklib'

module Chicago
  module ETL
    class RakeTasks < Rake::TaskLib
      def initialize(pipeline, db)
        @pipeline = pipeline
        @db = db
        define
      end

      def define
        namespace :etl do
          task :create_batch

          namespace :extract do
            @pipeline.defined_sources.each do |source|
              desc "Extract #{source.name.to_s.tr('_', ' ')}"
              task source.name => "etl:create_batch" do
                Extraction.new(source, @db).run(ETL_BATCH)
              end
              task :all => source.name
            end
          end
        end
      end
    end
  end
end
