require 'spec_helper'
require 'chicago/etl'

describe "Integrated ETL pipeline" do
  before :all do
    TEST_DB.drop_table(*(TEST_DB.tables))

    TEST_DB.create_table :users do
      primary_key :id
      varchar     :name
      timestamp   :updated_at
      timestamp   :created_at
    end
    
    TEST_DB.create_table :original_users do
      primary_key :id
      varchar     :name
      timestamp   :etl_extracted_at
    end

    Chicago::ETL::TableBuilder.build(TEST_DB)
    Chicago::ETL::Batch.db = TEST_DB
    Chicago.project_root = File.expand_path(File.join(File.dirname(__FILE__), ".."))
    tmpdir = File.expand_path(File.join(File.dirname(__FILE__), "..", "tmp"))
    FileUtils.rm_r(tmpdir) if File.exists?(tmpdir)
    
    TEST_DB.create_table :keys_dimension_user do
      integer :original_id
      integer :dimension_id
      primary_key [:original_id]
    end
    
    TEST_DB.create_table :dimension_user do
      primary_key :id
      integer :original_id, :null => false, :default => 0
      varchar :name, :null => false, :default => ''

      index :original_id, :unique => true
    end
    
    @schema = Chicago::StarSchema.new
    @schema.define_dimension(:user) do
      columns do
        integer :original_id
        string  :name
      end

      natural_key :original_id
    end

    @pipeline = Chicago::ETL::Pipeline::Pipeline.new
    @pipeline.define_source(:database, :users, :db => TEST_DB) do
      columns(:id, :name)
    end
  end
  
  it "should load the source users into the user dimension" do
    Timecop.freeze do
      TEST_DB[:users].insert(:name => "Roland", :created_at => Time.now, :updated_at => Time.now)
      TEST_DB[:users].insert(:name => "User 2", :created_at => Time.now - 1, :updated_at => Time.now - 1)
      
      batch = ETL::Batch.instance.start
      Chicago::ETL::Extraction.
        new(@pipeline.defined_sources[:users], TEST_DB).
        run(batch)
      batch.finish
      
      TEST_DB[:original_users].first[:id].should == 1
      TEST_DB[:original_users].first[:name].should == "Roland"
      TEST_DB[:original_users].first[:etl_extracted_at].should be_kind_of(Time)
      TEST_DB[:etl_extraction_times].where(:name => "users").
        get(:extract_from).should == Time.now

      TEST_DB[:users].insert(:name => "Another User", :created_at => Time.now, :updated_at => Time.now)

      TEST_DB[:users].where(:name => "Roland").update(:name => "Roland 2", :updated_at => Time.now + 1)

      TEST_DB[:users].where(:name => "User 2").update(:name => "Modified but not timestamped", :updated_at => Time.now - 1)
      
      batch = ETL::Batch.instance.start
      Chicago::ETL::Extraction.
        new(@pipeline.defined_sources[:users], TEST_DB).
        run(batch)
      batch.finish

      TEST_DB[:original_users].first[:name].should == "Roland 2"
      TEST_DB[:original_users].where(:name => "User 2").count.should == 1
      TEST_DB[:original_users].count.should == 3
    end
  end
end
