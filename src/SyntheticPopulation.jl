#module SyntheticPopulation

    #GLOBAL VARIABLES
    HOUSEHOLD_SIZE_COLUMN = :hh_size
    MINIMUM_ADULT_AGE = 23

    using StatsBase
    using DataFrames
    using ProportionalFitting
    using JSON3
    using ProgressMeter
    using GeoJSON
    using PolygonOps 
    using Distributions

    include("dataframe_preparation.jl")
    include("merge_distributions.jl")
    include("individual_allocation.jl")
    include("osm_boundaries_geojson.jl")
    include("area_allocation.jl")

    export generate_areas_dataframe, generate_joint_distributions, assign_areas_to_households! #functions

#end # module