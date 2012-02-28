require 'chicago/data/named_element_collection'
require 'chicago/etl/source_builder'
require 'chicago/etl/graph'

module Chicago
  module ETL
    extend self
    
    def define_source(type, name, db, &block)
      sources.add(SourceBuilder.new.build(type, name, {:db => db}, &block))
    end
    
    def sources
      @sources ||= Chicago::Data::NamedElementCollection.new
    end
  end
end
