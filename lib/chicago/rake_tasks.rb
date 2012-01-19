require 'rake/tasklib'

module Chicago
  # Rake tasks for a Chicago project.
  class RakeTasks < Rake::TaskLib
    def initialize(db, schema)
      @migration_dir = "migrations"
      @db = db
      @schema = schema
      @test_dir = "test"
      define
    end

    def define
      desc "Report code statistics (KLOCs, etc) from the application"
      task :stats do
        verbose = true
        stats_dirs = [['Code', './lib'], 
                      ['Test', "./#{@test_dir}"]].select { |name, dir| File.directory?(dir) }
        CodeStatistics.new(*stats_dirs).to_s
      end

      namespace :db do
        desc "Write Null dimension records"
        task :create_null_records do
          # TODO: replace this with proper logging.
          warn "Loading NULL records."
          @schema.dimensions.each {|dimension| dimension.create_null_records(@db) }
        end

        desc "Creates the etl tables"
        task :create_etl_tables do
          Chicago::ETL::TableBuilder.build(@db)
        end

        desc "Writes a migration file to change the database based on defined Facts & Dimensions"
        task :write_migrations do
          Database::MigrationFileWriter.new(@db, @migration_dir).
            write_migration_file(@schema)
        end
      end
    end
  end
end
