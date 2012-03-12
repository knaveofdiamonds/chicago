require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Source::Source do
  before :each do
    @db = mock(:staging_db)
  end

  it "should have a query to load records from a source into a staging table" do
    @source = described_class.new(:users,
                                  :columns => [:id, :name, :email])
    @db.should_receive(:run).
      with("LOAD DATA INFILE '/tmp/users.tsv' REPLACE INTO TABLE `original_users` (`id`,`name`,`email`)")    
    @source.stage(@db, "/tmp/users.tsv")
  end

  it "has columns" do
    columns = [:id, :name, :email]
    @source = described_class.new(:users, :columns => columns)
    @source.columns.should == columns
  end

  it "has a staging table" do
    @source = described_class.new(:users)
    @source.staging_table.should == :original_users
  end
end
