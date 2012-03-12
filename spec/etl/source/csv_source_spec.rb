require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Source::Csv do
  it "should be valid if a source_path has been set" do
    described_class.new(:users, :source_path => "/tmp/users.txt").should be_valid
    described_class.new(:users).should_not be_valid
  end

  it "should move the csv file path when extracting" do
    File.stub(:exists?).and_return(true)
    FileUtils.should_receive(:mv).with("/tmp/users.txt", "/tmp/destination")
    described_class.new(:users, :source_path => "/tmp/users.txt").
      extract("/tmp/destination")
  end

  it "should stage with CSV-style LOAD DATA options" do
    described_class.new(:users, :source_path => "/tmp/users.txt").load_data_options.should == "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES"
  end
end
