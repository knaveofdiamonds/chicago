module Chicago
  module ETL
    module Pipeline
      class Pipeline
        # A collection of explicitly defined sources.
        attr_reader :defined_sources

        # The source builder used by defined_source.
        attr_accessor :source_builder

        attr_accessor :default_source_type, :default_source_opts
        
        def initialize(options={})
          @nodes = {}
          @source_builder = Source::SourceBuilder.new
          @defined_sources = Chicago::Data::NamedElementCollection.new
          @default_source_type = options[:default_source_type]
          @default_source_opts = options[:default_source_opts]
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
        def define_source(*args, &block)
          if args.size == 1
            source = build_source_from_default(args.first, &block)
          else
            type, name, opts = *args
            source = @source_builder.build(type, name, opts, &block)
          end
          @defined_sources.add(source)
        end

        private

        def build_source_from_default(name, &block)
          @source_builder.build(default_source_type, name, default_source_opts, &block)
        end
      end
    end
  end
end
