require 'set'

module Chicago
  module ETL
    module Pipeline
      class Node
        attr_reader :out, :in

        def initialize(pipeline)
          @out = Set.new
          @in  = Set.new
          @marked = false
          pipeline.add(self)
        end

        def marked?
          @marked
        end

        def source?
          @in.empty?
        end

        def target?
          @out.empty?
        end
        
        def >(node)
          @out << node
          node.in << self
          node
        end

        def flowing_to?(node)
          connected?(node, :@out)
        end

        def flowing_from?(node)
          connected?(node, :@in)
        end

        def targets(flag = false)
          _traverse_to_end(false, :@out, :target?)
        end

        def sources
          _traverse_to_end(false, :@in, :source?)
        end

        def in_cycle?
          targets.include?(self)
        end
        
        protected

        def _traverse_to_end(not_first_node, var, method)
          if (not_first_node && send(method)) || @marked
            @marked = false
            Set.new([self])
          else
            @marked = true
            instance_variable_get(var).inject(Set.new) do |set, node|
              value = set | node._traverse_to_end(true, var, method)
              @marked = false
              value
            end
          end
        end
        
        def connected?(node, edges)
          nodes = instance_variable_get(edges)
          return true if nodes.include?(node)
          nodes.any? {|n| n.connected?(node, edges) }
        end
      end
    end
  end
end
