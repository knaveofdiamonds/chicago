require 'chicago/etl/pipeline/node'

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
    def initialize(pipeline, dimension)
      super pipeline
      @dimension = dimension
    end
    
    def name
      @dimension.name
    end
  end

  class FactNode < TableNode
    def initialize(pipeline, fact)
      super pipeline
      @fact = fact
    end
    
    def name
      @fact.name
    end
  end
end
