module Chicago
  module ETL
    module Pipeline
      class Pipeline
        # A collection of explicitly defined sources.
        attr_reader :defined_sources

        # The source builder used by defined_source.
        attr_accessor :source_builder
        
        def initialize
          @nodes = {}
          @source_builder = SourceBuilder.new
          @defined_sources = Chicago::Data::NamedElementCollection.new
        end

        def dimension_nodes
          @nodes[DimensionNode] || Set.new
        end
        
        def source_nodes
          @nodes[SourceNode] ||= Set.new
        end
        
        # Adds a node to the pipeline.
        #
        # This SHOULD NOT be called by clients - it should only be
        # called by instances of Node.
        def add(node)
          x = @nodes[node.class] ||= Set.new
          x << node
        end

        def nodes
          @nodes.values.inject {|a,b| a | b }
        end
        
        # Defines an explicit source for the ETL pipeline.
        def define_source(type, name, opts, &block)
          @defined_sources.add(@source_builder.build(type, name, opts, &block))
        end
      end
    end
  end
end
