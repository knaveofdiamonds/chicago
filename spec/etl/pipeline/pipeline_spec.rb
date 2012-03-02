require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::Pipeline do
  before :each do
    @pipeline = described_class.new
  end
  
  it "can have nodes added" do
    node = stub(:node)
    @pipeline.add(node)
    @pipeline.nodes.should == Set.new([node])
  end

  it "can have defined sources" do
    source = stub(:source, :name => :name)
    builder = mock(:source_builder)
    builder.should_receive(:build).with(:database, :name, {:foo => :bar}).and_return(source)
    @pipeline.source_builder = builder
    @pipeline.define_source(:database, :name, {:foo => :bar})
    @pipeline.defined_sources[:name].should == source
  end
end
