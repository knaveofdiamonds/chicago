require 'forwardable'
require 'chicago/etl/pipeline/node'
require 'chicago/schema/column_decorator'

module Chicago::ETL::Pipeline
  class TableNode < Node
    def table?
      true
    end
  end

  class SourceNode < TableNode
    # The name of this source
    attr_reader :name

    def initialize(pipeline, name)
      super pipeline
      @name = name
    end
  end

  class DimensionNode < TableNode
    extend Forwardable
    def_delegators :@dimension, :columns
    
    def initialize(pipeline, dimension)
      super pipeline
      @dimension = dimension
    end

    def propagate_columns
      @in.each do |in_node|
        columns.each {|column| in_node.add_column(column) }
      end
    end
    
    def name
      @dimension.name
    end
  end

  class FactNode < TableNode
    extend Forwardable
    def_delegators :@fact, :columns
    
    def initialize(pipeline, fact)
      super pipeline
      @fact = fact
    end
    
    def name
      @fact.name
    end
  end

  class RenamedColumn < Chicago::Schema::ColumnDecorator
    def initialize(column, expr)
      super column
      @expr = expr
    end
    
    def name
      @expr.expression
    end
  end
  
  class RenameNode < Node
    def initialize(pipeline, *names)
      super pipeline
      @renames = names.inject({}) {|hsh, expr|
        hsh[expr.aliaz] = expr
        hsh
      }
    end

    def add_column(column)
      if @renames[column.name]
        super rename_column(column, @renames[column.name])
      else
        super
      end
    end

    def rename_column(column, expr)
      RenamedColumn.new(column, expr)
    end
  end
end
