require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Graph do
  before :each do
    @schema = Chicago::StarSchema.new
  end

  it "maps from a source table to target table" do
    @schema.define_dimension(:user) do
      columns { string :name }
    end

    graph = Chicago::ETL::Graph.new(@schema, TEST_DB) do
      source(:users) > dimension(:user)
    end
    
    graph.extract_sequel.first.should select_columns([:id, :name])
    graph.load_sequel(TEST_DB).first.should == TEST_DB[:original_users].select(:name)
  end

  it "raises a MissingDefinitionError if the target dimension doesn't exist" do
    expect {
      Chicago::ETL::Graph.new(@schema, TEST_DB) do
        source(:users) > dimension(:user)
      end
    }.to raise_error(MissingDefinitionError)
  end
  
  it "maps from a source table to more than one target table" do
    @schema.define_dimension(:user) do
      columns { string :name }
    end

    @schema.define_dimension(:emails) do
      columns { string :email }
    end

    graph = Chicago::ETL::Graph.new(@schema, TEST_DB) do
      source(:users) > dimension(:user)
      source(:users) > dimension(:emails)
    end

    graph.extract_sequel.size.should == 1
    graph.extract_sequel.first.should select_columns([:id, :name, :email])
    graph.load_sequel(TEST_DB).first.should == TEST_DB[:original_users].select(:name)
    graph.load_sequel(TEST_DB).last.should == TEST_DB[:original_users].select(:email)
  end

  it "can rename a column after staging" do
    pending

    @schema.define_dimension(:user) do
      columns { string :name }
    end
    
    graph = Chicago::ETL::Graph.new(@schema, TEST_DB) do
      source(:users) > rename(:full_name.as(:name)) > dimension(:user)
    end

    graph.extract_sequel.first.should select_columns([:id, :full_name])
    graph.load_sequel(TEST_DB).first.should == TEST_DB[:original_users].select(:full_name.as(:name))
  end
end
