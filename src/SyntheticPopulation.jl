#module SyntheticPopulation

    ##################
    #GLOBAL VARIABLES#
    ##################
    #attributes
    HOUSEHOLD_SIZE_COLUMN = :hh_size
    POPULATION_COLUMN = :population
    ID_COLUMN = :id
    AGE_COLUMN = :age
    MARITALSTATUS_COLUMN = :maritalstatus
    SEX_COLUMN = :sex
    #values
    MINIMUM_ADULT_AGE = 23
    AVAILABLE_FOR_MARRIAGE = "Married"


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


    #######
    #FILES#
    #######
    include("dataframe_preparation.jl")
    include("merge_distributions.jl")
    include("individual_allocation.jl")
    include("osm_boundaries_geojson.jl")
    include("area_allocation.jl")


    ###########
    #FUNCTIONS#
    ###########
    export generate_areas_dataframe, generate_joint_distributions, assign_areas_to_households!

#end # module