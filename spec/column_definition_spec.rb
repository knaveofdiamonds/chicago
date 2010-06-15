require File.dirname(__FILE__) + "/spec_helper"

describe Chicago::ColumnDefinition do
  it "should have a name" do
    ColumnDefinition.new(:username, :string).name.should == :username
  end

  it "should have a column type" do
    ColumnDefinition.new(:username, :string).column_type.should == :string
  end

  it "should be equal to another column definition with the same attributes" do
    ColumnDefinition.new(:username, :string).should == ColumnDefinition.new(:username, :string)
  end

  it "should not be equal to another column definition with the different attributes" do
    ColumnDefinition.new(:username, :string).should_not == ColumnDefinition.new(:username, :integer)
  end

  it "should have a #min method" do
    ColumnDefinition.new(:username, :string, :min => 0 ).min.should == 0
  end

  it "should have a #max method" do
    ColumnDefinition.new(:username, :string, :max => 10 ).max.should == 10
  end

  it "should set min and max from an enumerable object's min and max" do
    column = ColumnDefinition.new(:username, :string, :range => 1..5 )
    column.min.should == 1
    column.max.should == 5
  end

  it "should forbid null values by default" do
    ColumnDefinition.new(:username, :string).should_not be_null
  end

  it "should allow you to accept non-null values" do
    ColumnDefinition.new(:username, :string, :null => true).should be_null
  end

  it "can define a set of valid elements" do
    ColumnDefinition.new(:username, :string, :elements => ['A', 'B']).elements.should == ['A', 'B']
  end

  it "can have a default value" do
    ColumnDefinition.new(:username, :string, :default => 'A').default.should == 'A'
  end
end

describe "A Hash returned by Chicago::ColumnDefinition#db_schema" do
  before :each do
    @tc = Chicago::Schema::TypeConverters::DbTypeConverter.for_db(stub(:database_type => :generic))
  end

  it "should have a :name entry" do
    ColumnDefinition.new(:username, :string, :max => 8).db_schema(@tc)[:name].should == :username
  end

  it "should have a :column_type entry" do
    ColumnDefinition.new(:username, :string, :max => 8).db_schema(@tc)[:column_type].should == :varchar
  end

  it "should not have a :default entry by default" do
    ColumnDefinition.new(:username, :string).db_schema(@tc).keys.should_not include(:default)
  end

  it "should have a :default entry if specified" do
    ColumnDefinition.new(:username, :string, :default => 'A').db_schema(@tc)[:default].should == 'A'
  end

  it "should have an :unsigned entry if relevant" do
    ColumnDefinition.new(:id, :integer, :min => 0).db_schema(@tc)[:unsigned].should be_true
  end

  it "should have an :entries entry if relevant" do
    ColumnDefinition.new(:username, :string, :elements => ['A']).db_schema(@tc)[:elements].should == ['A']
  end

  it "should not have an :entries entry if relevant" do
    ColumnDefinition.new(:username, :string).db_schema(@tc).keys.should_not include(:elements)
  end

  it "should have a :size entry if max is present and type is string" do
    ColumnDefinition.new(:username, :string, :max => 8).db_schema(@tc)[:size].should == 8
  end

  it "should have a default :size of [12,2] for money types" do
    ColumnDefinition.new(:username, :money).db_schema(@tc)[:size].should == [12,2]
  end
end
