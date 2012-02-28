module Chicago
  module ETL
    class Graph
      def initialize(schema, default_source, &block)
        @schema = schema
        @connections = []
        @default_source = default_source
        graph &block if block_given?
      end

      def graph(&block)
        instance_eval(&block)
      end

      def source(name)
        NamedNode.new(self, name)
      end

      def dimension(name)
        check_dimension_exists(name)        
        NamedNode.new(self, name)
      end

      def rename(column)
        NamedNode.new(self, name)
      end

      def add_connection(connection)
        @connections << connection
      end

      def extract_sequel
        @connections.inject({}) do |hsh, connection|
          columns = @schema.dimension(connection.target.name).columns.map(&:name)
          hsh[connection.source.name] ||= Set.new([:id])
          hsh[connection.source.name] += columns
          hsh
          
        end.map do |(k, v)|
          @default_source[k].select(*(v.to_a))
        end
      end

      def load_sequel(db)
        @connections.map do |connection|
          columns = @schema.dimension(connection.target.name).columns.map(&:name)
          db[:"original_#{connection.source.name}"].select(*columns)
        end
      end

      private

      def check_dimension_exists(name)
        unless @schema.dimension(name)
          raise MissingDefinitionError.new("Dimension #{name} does not exist in this schema.")
        end
        true
      end
    end
    
    class GraphNode
      def initialize(graph)
        @graph = graph
      end
      
      def >(other)
        @graph.add_connection GraphNodeConnection.new(@graph, self, other)
      end
    end

    class NamedNode < GraphNode
      attr_reader :name
      
      def initialize(graph, name)
        super graph
        @name = name
      end
    end

    
    class GraphNodeConnection < GraphNode
      attr_reader :source, :target
      
      def initialize(graph, source, target)
        super graph
        @source = source
        @target = target
      end
    end
  end
end
