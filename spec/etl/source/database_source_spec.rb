require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Source::Database do
  before :each do
    @db = mock(:db).as_null_object
    @dataset = mock(:dataset)
    @ds = lambda {|ds| @dataset }
  end

  it "disconnects from the database before extracting to avoid fork problems" do
    @db.should_receive(:disconnect)
    described_class.new(:users, :db => @db).extract("/tmp/nonexistent.txt")
  end

  it "returns a SQL query from query" do
    described_class.new(:users, :db => TEST_DB).query.
      should =~ /^SELECT.*FROM `users`/    
  end

  it "dumps the query into an outfile when extracting" do
    source = described_class.new(:users, :db => TEST_DB)
    TEST_DB.
      should_receive(:run).
      with(source.query + " INTO OUTFILE '/tmp/blah.tsv'")
    source.extract("/tmp/blah.tsv")
  end

  it "is valid if the database table exists" do
    @db.stub(:table_exists?).with(:users).and_return(true)
    @db.stub(:table_exists?).with(:customers).and_return(false)

    described_class.new(:users, :db => @db).should be_valid
    described_class.new(:customers, :db => @db).should_not be_valid
  end
  
  it "returns a time qualified query when given an extract time" do
    query = described_class.new(:users, :db => TEST_DB).query(Time.local(2011,01,01,12,0,0))
    query.should =~ /`users`\.`created_at` >= '2011-01-01 12:00:00'/
    query.should =~ /`users`\.`updated_at` >= '2011-01-01 12:00:00'/
  end

  it "allows overriding default timestamps considered for diff extract" do
    query = described_class.new(:users,
                                :db => TEST_DB,
                                :timestamps => [:other__updated_at]).query(Time.local(2011,01,01,12,0,0))
    query.should_not =~ /`users`\.`created_at` >= '2011-01-01 12:00:00'/
    query.should_not =~ /`users`\.`updated_at` >= '2011-01-01 12:00:00'/
    query.should =~ /`other`\.`updated_at` >= '2011-01-01 12:00:00'/
  end
end
