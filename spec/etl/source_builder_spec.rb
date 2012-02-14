require 'spec_helper'
require 'chicago/etl/source_builder'

describe Chicago::ETL::SourceBuilder, "builds a Source that" do
  before :each do
    @schema = Chicago::StarSchema.new
    @db = stub(:default_source_db).as_null_object
  end

  it "has a name" do
    subject.build(:users, @db).name.should == :users
  end
  
  it "can have columns defined" do
    source = subject.build(:users, @db) do
      columns(:id, :name)
    end
    source.columns.should == [:id, :name]
  end

  it "is valid if the table exists in the source database" do
    @db.should_receive(:table_exists?).with(:users).and_return(true)
    subject.build(:users, @db).should be_valid
  end

  it "is not valid if the table doesn't exist in the source database" do
    @db.should_receive(:table_exists?).with(:users).and_return(false)
    subject.build(:users, @db).should_not be_valid
  end

  it "can have an explicit table name defined, different from name" do
    @db.should_receive(:table_exists?).with(:users).and_return(true)

    subject.build(:visitors, @db) do
      table_name :users
    end.should be_valid
  end

  describe "from a dimension" do
    before :each do
      @dimension = @schema.define_dimension(:user) do
        columns do
          integer :original_id
          string  :name
        end
      end
    end

    it "can be built from a Dimension, with an assumed name" do
      subject.build(@dimension, @db).name.should == :users
    end

    it "takes a column from the Dimension" do
      subject.build(@dimension, @db).columns.should include(:name)
    end

    it "assumes original_id == id in the source" do
      subject.build(@dimension, @db).columns.should_not include(:original_id)
      subject.build(@dimension, @db).columns.should include(:id)
    end
  end
end
