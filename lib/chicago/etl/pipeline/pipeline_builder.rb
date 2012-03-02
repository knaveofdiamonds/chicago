require 'chicago/etl/pipeline/pipeline'
require 'chicago/etl/pipeline/node'

module Chicago
  module ETL
    module Pipeline
      class NodeFactory
        def initialize(pipeline, schema)
          @pipeline = pipeline
          @schema = schema
        end

        def build(&block)
          instance_eval(&block)
          @pipeline
        end

        def source(name)
          SourceNode.new(@pipeline, name)
        end

        def dimension(name)
          DimensionNode.new(@pipeline, @schema.dimension(name))
        end

        def fact(name)
          FactNode.new(@pipeline, @schema.fact(name))
        end
      end
    end
  end
end
