require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::ErrorHandler do
  before :each do
    @db = mock(:db).as_null_object
    @handler = described_class.new(@db)
  end

  it "inserts an error into the log" do
    @db.stub(:[]).and_return(@db)
    @db.should_receive(:insert).with(:error => :test_error)
    @handler.error(:error => :test_error)
  end
end
