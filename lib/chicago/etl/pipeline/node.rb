require 'set'
require 'chicago/etl/pipeline/connection'

module Chicago
  module ETL
    module Pipeline
      class Node
        attr_reader :downstream, :upstream, :columns

        def initialize(pipeline, data=nil)
          @downstream = Set.new
          @upstream  = Set.new
          @columns = Chicago::Data::NamedElementCollection.new
          @data = data
          pipeline.add(self)
        end

        def add_column(column)
          @columns.add(column)
          @upstream.each do |in_node|
            in_node.add_column(column)
          end
        end

        def insert(node)
          @downstream.each do |n|
            n.upstream.delete(self)
            node > n
          end
          @downstream.clear          
          self > node
        end
        
        def out
          self
        end

        def in
          self
        end

        def origin?
          @upstream.empty?
        end

        def target?
          @downstream.empty?
        end

        def table?
          false
        end
        
        def >(node)
          Connection.new(self, node.in)
        end

        def <(node)
          Connection.new(node.out, self)
        end
        
        def flowing_to?(node)
          connected?(node, :@downstream)
        end

        def flowing_from?(node)
          connected?(node, :@upstream)
        end

        def targets
          Set.new(DepthFirstIterator.downstream(self).select(&:target?)) - Set.new([self])
        end

        def origins
          Set.new(DepthFirstIterator.upstream(self).select(&:origin?)) - Set.new([self])
        end

        def in_cycle?
          DepthFirstIterator.downstream(self).has_cycles?
        end
        
        protected
        
        def connected?(node, edges)
          nodes = instance_variable_get(edges)
          return true if nodes.include?(node)
          nodes.any? {|n| n.connected?(node, edges) }
        end
      end
    end
  end
end
