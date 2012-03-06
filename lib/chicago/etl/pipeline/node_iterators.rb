module Chicago
  module ETL
    module Pipeline
      class NodeIterator
        include Enumerable
        
        def initialize(node, direction)
          @node = node
          @direction = direction
        end

        def self.downstream(node)
          new(node, :downstream)
        end

        def self.upstream(node)
          new(node, :upstream)
        end

        def has_cycles?
          @seen = Set.new()
          @fifo = [@node]

          until @fifo.empty?
            node = @fifo.send(next_node)
            if @seen.include?(node)
              return true
            else
              @seen << node
              @fifo += node.send(@direction).to_a
            end
          end

          false
        end
        
        def each(&block)
          @seen = Set.new
          @fifo = [@node]

          until @fifo.empty?
            node = @fifo.send(next_node)
            unless @seen.include?(node)
              @seen << node
              @fifo += node.send(@direction).to_a
              yield node
            end
          end
        end
      end
      
      class DepthFirstIterator < NodeIterator
        def next_node
          :pop
        end
      end

      class BreadthFirstIterator < NodeIterator
        def next_node
          :shift
        end
      end
    end
  end
end
