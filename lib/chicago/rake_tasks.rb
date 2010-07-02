require 'rake/tasklib'

module Chicago
  # Rake tasks for a Chicago project.
  class RakeTasks < Rake::TaskLib
    def initialize(db)
      @migration_dir = "db/migrate"
      @db = db
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

      namespace :schema do
        desc "Writes a migration file to change the database based on defined Facts & Dimensions"
        task :write_migrations do
          Schema::MigrationFileWriter.new(@db, @migration_dir).write_migration_file
        end
      end
    end
  end
end