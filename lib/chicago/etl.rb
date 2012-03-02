require 'chicago/data/named_element_collection'
require 'chicago/etl/source_builder'
require 'chicago/etl/graph'
require 'chicago/etl/batch_execution'
require 'chicago/etl/pipeline/pipeline'
require 'chicago/etl/pipeline/node'
require 'chicago/etl/pipeline/source_node'
require 'chicago/etl/pipeline/pipeline_builder'

module Chicago
  module ETL
    extend self

    def pipeline
      @pipeline ||= Pipeline::Pipeline.new
    end
    
    def define_source(type, name, db, &block)
      pipeline.define_source(type, name, {:db => db}, &block)
    end
    
    def sources
      pipeline.defined_sources
    end
  end
end
