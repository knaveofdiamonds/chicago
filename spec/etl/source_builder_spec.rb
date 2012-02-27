require 'spec_helper'
require 'chicago/etl/source_builder'

describe Chicago::ETL::SourceBuilder, "builds a Source that" do
  before :each do
    @schema = Chicago::StarSchema.new
    @db = stub(:default_source_db).as_null_object
  end

  it "has a name" do
    subject.build(:database, :users, :db => @db).name.should == :users
  end
  
  it "can have columns defined" do
    source = subject.build(:database, :users, :db => @db) do
      columns(:id, :name)
    end
    source.columns.should == [:id, :name]
  end

  it "can modify the dataset to join  or filter" do
    source = subject.build(:database, :users, :db => TEST_DB) do
      columns(:id, :name)

      dataset do |ds|
        ds.where(:name => "Foo")
      end
    end
    
    source.dataset.sql.should =~ /WHERE \(`name` = 'Foo'\)/
  end
  
  it "has all the table's columns defined by default" do
    dataset = mock()
    dataset.should_receive(:columns).and_return([:id, :name])
    @db.stub(:[]).with(:users).and_return(dataset)
    subject.build(:database, :users, :db => @db).columns.
      should == [:id, :name]
  end
  
  it "is valid if the table exists in the source database" do
    @db.should_receive(:table_exists?).with(:users).and_return(true)
    subject.build(:database, :users, :db => @db).should be_valid
  end

  it "is not valid if the table doesn't exist in the source database" do
    @db.should_receive(:table_exists?).with(:users).and_return(false)
    subject.build(:database, :users, :db => @db).should_not be_valid
  end

  it "can have an explicit table name defined, different from name" do
    @db.should_receive(:table_exists?).with(:users).and_return(true)

    subject.build(:database, :visitors, :db => @db) do
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
      subject.build(:database, @dimension, :db => @db).name.should == :users
    end

    it "takes a column from the Dimension" do
      subject.build(:database, @dimension, :db => @db).columns.
        should include(:name)
    end

    it "assumes original_id == id in the source" do
      subject.build(:database, @dimension, :db => @db).columns.
        should_not include(:original_id)
      subject.build(:database, @dimension, :db => @db).columns.
        should include(:id)
    end
  end

  describe "from a fact" do
    before :each do
      @dimension = @schema.define_dimension(:user)

      @fact = @schema.define_fact(:sales) do
        dimensions :user

        degenerate_dimensions do
          integer :original_id
        end

        measures do
          integer :amount
        end
      end
    end

    it "can be built from a Fact, with an assumed name" do
      subject.build(:database, @fact, :db => @db).name.should == :sales
    end

    it "has columns" do
      subject.build(:database, @fact, :db => @db).columns.
        should == [:id, :user_id, :amount]
    end
  end
end
