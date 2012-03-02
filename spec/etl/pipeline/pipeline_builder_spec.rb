require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::NodeFactory do
  before :each do
    @schema = mock(:schema)
    @pipeline = Chicago::ETL::Pipeline::Pipeline.new
  end
  
  it "should build a source node" do
    pipeline = described_class.new(@pipeline, @schema).build do
      source(:foo)
    end

    node = pipeline.nodes.first
    node.should be_kind_of(Chicago::ETL::Pipeline::SourceNode)
  end

  it "should build a dimension node" do
    @schema.should_receive(:dimension).with(:foo).and_return(stub(:dimension, :name => :foo))

    pipeline = described_class.new(@pipeline, @schema).build do
      dimension(:foo)
    end

    node = pipeline.nodes.first
    node.should be_kind_of(Chicago::ETL::Pipeline::DimensionNode)
  end

  it "should build a fact node" do
    @schema.should_receive(:fact).with(:foo).and_return(stub(:fact, :name => :foo))

    pipeline = described_class.new(@pipeline, @schema).build do
      fact(:foo)
    end

    node = pipeline.nodes.first
    node.should be_kind_of(Chicago::ETL::Pipeline::FactNode)
  end
end
