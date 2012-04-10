require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::DimensionScreen do
  before :each do
    @schema = Chicago::StarSchema.new
    dimension = @schema.define_dimension(:user) do
      columns do
        string :name, :max => 100
        string :user_type, :elements => ['A', 'B', 'Unknown'], :default => "Unknown"
        string :foo, :elements => ['A', 'B']
        string :bar, :elements => ['A', 'B'], :null => true
      end
    end

    @error  = stub(:error_record).as_null_object
    @screen = described_class.new(dimension, @error)
  end

  it "replaces an element with the default if it doesn't exist in elements" do
    @screen.screen(:id => 1, :user_type => "C")[:user_type].should == "Unknown"
  end

  it "logs an incorrect element as invalid" do
    @error.should_receive(:error).
      with(:process_name => "Chicago::ETL::DimensionScreen",
           :process_version => 1,
           :table => :dimension_user,
           :field => :user_type,
           :field_id => 1,
           :error => :invalid_field_value,
           :severity => 2,
           :error_detail => "C")
    @screen.screen(:id => 1, :user_type => "C")[:user_type]
  end

  it "logs a missing value" do
    @error.should_receive(:error).
      with(:process_name => "Chicago::ETL::DimensionScreen",
           :process_version => 1,
           :table => :dimension_user,
           :field => :user_type,
           :field_id => 1,
           :error => :missing_field_value,
           :severity => 2)

    @screen.screen(:id => 1)[:user_type]
  end
end
