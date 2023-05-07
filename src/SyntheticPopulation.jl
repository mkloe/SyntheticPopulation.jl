module SyntheticPopulation

    ###########
    #FUNCTIONS#
    ###########
    export generate_areas_dataframe
    export generate_joint_distribution
    export assign_individuals_to_households
    export assign_areas_to_households!
    
    ##########
    #PACKAGES#
    ##########
    using StatsBase
    using DataFrames
    using ProportionalFitting
    using JSON3
    using ProgressMeter
    using GeoJSON
    using PolygonOps 
    using Distributions
    using Downloads 
    using TranscodingStreams
    using CodecZlib

    #######
    #FILES#
    #######
    include("dataframe_preparation.jl")
    include("merge_distributions.jl")
    include("individual_allocation.jl")
    include("osm_boundaries_geojson.jl")
    include("area_allocation.jl")
    
    ##################
    #GLOBAL VARIABLES#
    ##################
    #attributes
    const HOUSEHOLD_SIZE_COLUMN = :hh_size
    const POPULATION_COLUMN = :population
    const ID_COLUMN = :id
    const AGE_COLUMN = :age
    const MARITALSTATUS_COLUMN = :maritalstatus
    const SEX_COLUMN = :sex
    #values
    const MINIMUM_ADULT_AGE = 23
    const AVAILABLE_FOR_MARRIAGE = "Married"

end # module