require 'chicago/data/named_element_collection'
require 'chicago/etl/source_builder'

module Chicago
  module ETL
    extend self
    
    def define_database_source(name, db, &block)
      sources.add(SourceBuilder.new.build(name, {:db => db}, &block))
    end
    
    def sources
      @sources ||= Chicago::Data::NamedElementCollection.new
    end
  end
end
