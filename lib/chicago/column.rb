require 'chicago/schema/named_element'

module Chicago
  # A column in a dimension or fact record.
  #
  # The column definition is used to generate the options
  # to create the column in the database schema, but also
  # to provide an abstract definition of the column for views
  # and other Data Warehouse code.
  #
  # You shouldn't need to create a Column manually - they
  # are generally defined using the schema definition DSL.
  class Column
    include Schema::NamedElement

    # Creates a new column definition.
    #
    # name::  the name of the column.
    # column_type::  the abstract type of the column. For example, :string.
    #
    # Options:
    #
    # min::      the minimum length/number of this column.
    # max::      the maximum length/number of this column.
    # range::    any object with a min & max method - overrides min/max (above).
    # null::     whether this column can be null. False by default.
    # elements:: the allowed values this column can take.
    # default::  the default value for this column. 
    # descriptive:: whether this column is purely descriptive and
    # won't be used for grouping/filtering.
    # semi_additive:: whether a measure column is semi_additive.
    def initialize(name, column_type, opts={})
      @opts = normalize_opts(column_type, opts)
      
      super name, opts
      
      @column_type = column_type
      @countable_label = @opts[:countable].kind_of?(String) ? @opts[:countable] : @label
      @countable   = !! @opts[:countable]
      @min         = @opts[:min]
      @max         = @opts[:max]
      @null        = @opts[:null]
      @elements    = @opts[:elements]
      @default     = @opts[:default]
      @descriptive = !! @opts[:descriptive]
      @semi_additive = !! @opts[:semi_additive]
      @internal    = !! @opts[:internal]
    end

    # Returns the type of this column. This is an abstract type,
    # not a database type (for example :string, not :varchar).
    attr_reader :column_type

    # Returns the minimum value of this column, or nil.
    attr_reader :min
    
    # Returns the minimum value of this column, or nil.
    attr_reader :max
    
    # Returns an Array of allowed elements, or nil.
    attr_reader :elements
    
    # Returns the default value for this column, or nil.
    attr_reader :default

    attr_reader :countable_label

    # Returns true if this column can be counted.
    def countable?
      @countable
    end
    
    # Returns true if this column should be ignored in user-facing
    # parts of an application
    def internal?
      @internal
    end

    # Returns true if this measure column can be averaged, but not
    # summed.
    def semi_additive?
      @semi_additive
    end

    # Returns true if null values are allowed.
    def null?
      @null
    end
    
    # Returns true if this column is just informational, and is not
    # intended to be used as a filter.
    def descriptive?
      @descriptive
    end

    # Returns true if both definition's attributes are equal.
    def ==(other)
      other.kind_of?(self.class) && 
        name == other.name && 
        column_type == other.column_type && 
        @opts == other.instance_variable_get(:@opts)
    end

    # Returns true if this column stores a numeric value.
    def numeric?
      @numeric ||= [:integer, :money, :percent, :decimal, :float].include?(column_type)
    end

    def hash #:nodoc:
      name.hash
    end

    private
    
    def normalize_opts(type, opts)
      opts = {:null => default_null(type), :min => default_min(type)}.merge(opts)
      if opts[:range]
        opts[:min] = opts[:range].min
        opts[:max] = opts[:range].max
        opts.delete(:range)
      end
      opts
    end

    def default_null(type)
      [:date, :timestamp, :datetime].include?(type)
    end

    def default_min(type)
      0 if type == :money
    end
  end
end
