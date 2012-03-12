require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Extraction do
  before :each do
    @source = stub(:source, :name => :foo).as_null_object
    @db = stub(:db).as_null_object
  end
  
  it "runs its task in the context of an ETL batch" do
    etl_batch = mock(:batch)
    etl_batch.should_receive(:perform_task).with(:extract, :foo)
    described_class.new(@source, @db).run(etl_batch)
  end
end
