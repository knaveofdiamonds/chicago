require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::SourceNode do
  before :each do
    @pipeline = Chicago::ETL::Pipeline::Pipeline.new
  end
  
  it "should be a table" do
    described_class.new(@pipeline, :foo).should be_table
  end
end

describe Chicago::ETL::Pipeline::DimensionNode do
  before :each do
    @dimension = mock(:dimension)
    @pipeline = Chicago::ETL::Pipeline::Pipeline.new
  end
  
  it "should be a table" do
    described_class.new(@pipeline, @dimension).should be_table
  end

  it "should have the dimension's name" do
    @dimension.should_receive(:name).and_return(:foo)
    described_class.new(@pipeline, @dimension).name.should == :foo
  end
end
