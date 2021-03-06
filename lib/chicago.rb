# Requires go here
require 'chronic'
require 'sequel'
require 'sequel/extensions/inflector'
Sequel.extension(:core_extensions)

# TODO: move this back to the Sequel MySQL adapter
require 'chicago/core_ext/sequel/dataset'

require 'chicago/core_ext/hash'
require 'chicago/data/month'

require 'chicago/star_schema'
require 'chicago/database/constants'
require 'chicago/database/index_generator'
require 'chicago/database/concrete_schema_strategies'
require 'chicago/database/migration_file_writer'
require 'chicago/database/schema_generator'
require 'chicago/query'

module Chicago
  class << self
    # The root directory for the project.
    attr_accessor :project_root
  end
  
  # @api private
  module Database
  end
  
  ### Autoloads
  autoload :RakeTasks, 'chicago/rake_tasks'
end
