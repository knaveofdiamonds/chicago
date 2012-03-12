$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'chicago'
require 'rspec'
require 'timecop'
require 'yaml'
require 'rspec/autorun'

include Chicago

unless defined? TEST_DB
  TEST_DB_CONFIG = YAML.load(File.read(File.dirname(__FILE__) + "/db_connections.yml"))
  TEST_DB = Sequel.connect(TEST_DB_CONFIG)
end

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

RSpec.configure do |config|
end

class StubBatch
  def initialize
    @log = stub(:etl_log)
  end

  def self.instance
    @instance ||= self.new
  end

  def start
    self
  end

  def dir
    "/tmp"
  end
  
  def perform_task(*args)
    yield
  end

  attr_reader :log
end
