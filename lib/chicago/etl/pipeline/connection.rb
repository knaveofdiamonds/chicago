require 'forwardable'

module Chicago
  module ETL
    module Pipeline
      class Connection
        extend Forwardable
        
        def_delegators :@out, :out, :downstream
        def_delegators :@in, :in, :upstream
        
        def initialize(in_node, out_node)
          @in, @out = in_node, out_node
          @in.downstream << @out.in
          @out.upstream << @in.out
        end

        def >(node)
          Connection.new(self, node)
        end

        def <(node)
          Connection.new(node, self)
        end
      end
    end
  end
end
