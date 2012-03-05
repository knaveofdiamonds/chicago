require 'spec_helper'
require 'chicago/etl'

describe "Propagating columns back through the pipeline" do
  before :all do
    @schema = Chicago::StarSchema.new
    @schema.define_dimension(:user) do
      columns do
        integer :original_id
        string  :name
      end

      natural_key :original_id
    end

    @schema.define_dimension(:email) do
      columns do
        string  :email
      end
    end
  end

  before :each do
    @pipeline = Chicago::ETL::Pipeline::Pipeline.new
    @builder = Chicago::ETL::Pipeline::NodeFactory.new(@pipeline, @schema)
  end

  it "should propogate a simple column back from a dimension to a source" do
    @builder.build do
      source(:users) > dimension(:user)
    end
    
    @pipeline.source_nodes.first.columns[:name].should_not be_nil
  end

  it "combines columns from multiple dimensions into one source" do
    @builder.build do
      source(:users) > dimension(:user)
      source(:users) > dimension(:email)
    end

    @pipeline.dimension_nodes.size.should == 2
    @pipeline.source_nodes.size.should == 1
    @pipeline.source_nodes.first.columns.size.should == 3
  end

  it "propagates columns through intermediate nodes" do
    @builder.build do
      source(:users) > transform(:foo) > dimension(:user)
    end

    @pipeline.source_nodes.first.columns[:name].should_not be_nil
  end

  it "copes with renames" do
    @builder.build do
      source(:users) >
        rename(:full_name.as(:name)) >
        dimension(:user)
    end

    @pipeline.source_nodes.first.columns[:full_name].should_not be_nil
  end

  it "copes with renames" do
    @builder.build do
      source(:users) >
        rename(:full_name.as(:foo)) >
        rename(:foo.as(:name)) >
        dimension(:user)
    end

    @pipeline.source_nodes.first.columns[:full_name].should_not be_nil
  end

  it "should rename id to original_id automatically" do
    pending
    @builder.build do
      source(:users) > dimension(:user)
    end

    @pipeline.source_nodes.first.columns[:id].should_not be_nil
  end
end
