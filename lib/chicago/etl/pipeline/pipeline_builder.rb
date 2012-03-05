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
          @pipeline.dimension_nodes.each(&:propagate_columns)
          @pipeline
        end

        def source(name)
          n = @pipeline.source_nodes.detect {|n| n.name == name }
          n || SourceNode.new(@pipeline, name)
        end

        def dimension(name)
          rn = RenameNode.new(@pipeline, :id.as(:original_id))
          dn = DimensionNode.new(@pipeline, @schema.dimension(name))
          rn > dn
        end

        def transform(name)
          Node.new(@pipeline)
        end

        def rename(renames)
          RenameNode.new(@pipeline, renames)
        end

        def fact(name)
          FactNode.new(@pipeline, @schema.fact(name))
        end
      end
    end
  end
end
