require File.dirname(__FILE__) + "/spec_helper"

describe Chicago::Column do
  it "should have a name" do
    Column.new(:username, :string).name.should == :username
  end

  it "should have a column type" do
    Column.new(:username, :string).column_type.should == :string
  end

  it "should be equal to another column definition with the same attributes" do
    Column.new(:username, :string).should == Column.new(:username, :string)
  end

  it "should not be equal to another column definition with the different attributes" do
    Column.new(:username, :string).should_not == Column.new(:username, :integer)
  end

  it "should have a #min method" do
    Column.new(:username, :string, :min => 0 ).min.should == 0
  end

  it "should have a min of 0 by default for money columns" do
    Column.new(:username, :money).min.should == 0
  end

  it "should have a #max method" do
    Column.new(:username, :string, :max => 10 ).max.should == 10
  end

  it "should set min and max from an enumerable object's min and max" do
    column = Column.new(:username, :string, :range => 1..5 )
    column.min.should == 1
    column.max.should == 5
  end

  it "should forbid null values by default" do
    Column.new(:username, :string).should_not be_null
  end

  it "should allow you to accept non-null values" do
    Column.new(:username, :string, :null => true).should be_null
  end

  it "should allow null values by default for date, datetime or timestamp columns" do
    Column.new(:username, :timestamp).should be_null
    Column.new(:username, :date).should be_null
    Column.new(:username, :datetime).should be_null
  end

  it "can define a set of valid elements" do
    Column.new(:username, :string, :elements => ['A', 'B']).elements.should == ['A', 'B']
  end

  it "can have a default value" do
    Column.new(:username, :string, :default => 'A').default.should == 'A'
  end

  it "should have a descriptive? method, false by default" do
    Column.new(:username, :string).should_not be_descriptive
  end

  it "should be definable as descriptive" do
    Column.new(:username, :string, :descriptive => true).should be_descriptive
  end

  it "should be numeric if an integer" do
    Column.new(:username, :integer).should be_numeric
  end

  it "should be numeric if a money" do
    Column.new(:username, :money).should be_numeric
  end

  it "should be numeric if a float" do
    Column.new(:username, :float).should be_numeric
  end

  it "should be numeric if a decimal" do
    Column.new(:username, :decimal).should be_numeric
  end

  it "should be numeric if a percentage" do
    Column.new(:username, :percent).should be_numeric
  end

  it "should not be numeric if a string" do
    Column.new(:username, :string).should_not be_numeric
  end
end

describe "A Hash returned by Chicago::Column#db_schema" do
  before :each do
    @tc = Chicago::Schema::TypeConverters::DbTypeConverter.for_db(stub(:database_type => :generic))
  end

  it "should have a :name entry" do
    Column.new(:username, :string, :max => 8).db_schema(@tc)[:name].should == :username
  end

  it "should have a :column_type entry" do
    Column.new(:username, :string, :max => 8).db_schema(@tc)[:column_type].should == :varchar
  end

  it "should not have a :default entry by default" do
    Column.new(:username, :string).db_schema(@tc).keys.should_not include(:default)
  end

  it "should have a :default entry if specified" do
    Column.new(:username, :string, :default => 'A').db_schema(@tc)[:default].should == 'A'
  end

  it "should have an :unsigned entry if relevant" do
    Column.new(:id, :integer, :min => 0).db_schema(@tc)[:unsigned].should be_true
  end

  it "should have an :entries entry if relevant" do
    Column.new(:username, :string, :elements => ['A']).db_schema(@tc)[:elements].should == ['A']
  end

  it "should not have an :entries entry if relevant" do
    Column.new(:username, :string).db_schema(@tc).keys.should_not include(:elements)
  end

  it "should have a :size entry if max is present and type is string" do
    Column.new(:username, :string, :max => 8).db_schema(@tc)[:size].should == 8
  end

  it "should have a default :size of [12,2] for money types" do
    Column.new(:some_value, :money).db_schema(@tc)[:size].should == [12,2]
  end

  it "should be unsigned by default if a percentage" do
    Column.new(:some_value, :percent).db_schema(@tc)[:unsigned].should be_true
  end

  it "should have a default :size of [6,3] for percent types" do
    Column.new(:rate, :percent).db_schema(@tc)[:size].should == [6,3]
  end

  it "should have a :size that is set explictly" do
    Column.new(:username, :money, :size => 'huge').db_schema(@tc)[:size].should == 'huge'
  end

  it "should explicitly set the default to nil for timestamp columns" do
    Column.new(:username, :timestamp).db_schema(@tc).has_key?(:default).should be_true
    Column.new(:username, :timestamp).db_schema(@tc)[:default].should be_nil
  end
end