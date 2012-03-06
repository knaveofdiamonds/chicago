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
    def name
      @data
    end
  end

  class DimensionNode < TableNode
    extend Forwardable
    def_delegators :@data, :columns
    
    def propagate_columns
      @upstream.each do |in_node|
        columns.each {|column| in_node.add_column(column) }
      end
    end
    
    def name
      @data.name
    end
  end

  class FactNode < TableNode
    extend Forwardable
    def_delegators :@data, :columns
        
    def name
      @data.name
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
      data = names.inject({}) {|hsh, expr|
        hsh[expr.aliaz] = expr
        hsh
      }
      super pipeline, data
    end

    def add_column(column)
      if @data[column.name]
        super rename_column(column, @data[column.name])
      else
        super
      end
    end

    def rename_column(column, expr)
      RenamedColumn.new(column, expr)
    end
  end
end
