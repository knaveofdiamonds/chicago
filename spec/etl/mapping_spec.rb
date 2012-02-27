require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Source do
  before :each do
    @db = stub(:default_source_db).as_null_object
  end

  it "adds a source to a collection of sources" do
    source = Chicago::ETL.define_source(:database, :users, @db)
    Chicago::ETL.sources[:users].should == source
    Chicago::ETL.sources.to_a.should == [source]
  end
end
