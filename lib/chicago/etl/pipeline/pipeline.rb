module Chicago
  module ETL
    module Pipeline
      class Pipeline
        # Adds a node to the pipeline.
        #
        # This SHOULD NOT be called by clients - it should only be
        # called by instances of Node.
        def add(node)
          @nodes << node
        end
      end

      class PipelineBuilder
        def initialize(pipeline)
          @pipeline = pipeline
        end

        def define_nodes(&block)
          instance_eval(&block)
        end

        protected

        def source(name)
          Node.new(@pipeline)
        end

        def dimension(name)
          Node.new(@pipeline)
        end
      end
    end
  end
end
