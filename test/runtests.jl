using Test
using DataFrames
using GeoJSON
include("../src/SyntheticPopulation.jl")
include("data/prepare_test_data.jl")
using .SyntheticPopulation


@testset "general" begin
    
    #############################
    #tests for the main function#
    #generate_joint_distribution#
    #############################

    #tests for generate_joint_distribution without config
    joint_distribution = generate_joint_distribution(
                                marginal_ind_age_sex, 
                                marginal_ind_sex_maritalstatus, 
                                marginal_ind_income)
    @test joint_distribution[165, SyntheticPopulation.MARITALSTATUS_COLUMN] == "Married"
    @test nrow(joint_distribution) == 720
    @test length(collect(unique(joint_distribution[:,SyntheticPopulation.AGE_COLUMN]))) == length(collect(unique(marginal_ind_age_sex[:,SyntheticPopulation.AGE_COLUMN])))
    
    #tests for no configuration
    no_config_joint_distribution = generate_joint_distribution(
                                    marginal_ind_age_sex, 
                                    marginal_ind_sex_maritalstatus, 
                                    marginal_ind_income;
                                    config_file = "test/data/test_no_config.json")
    @test joint_distribution == no_config_joint_distribution
    
    #tests for missing_config
    missing_config_joint_distribution = generate_joint_distribution(
                                            marginal_ind_age_sex, 
                                            marginal_ind_sex_maritalstatus, 
                                            marginal_ind_income;
                                            config_file = "test/data/test_missing_config.json")
    missing_rows = filter(SyntheticPopulation.MARITALSTATUS_COLUMN => x -> typeof(x) == Missing, missing_config_joint_distribution)
    @test any(map(val -> val[1] == (true), eachcol(mapcols(x -> any(ismissing, x), missing_config_joint_distribution)))) == true
    @test sum(missing_rows[:,SyntheticPopulation.POPULATION_COLUMN]) == 3230
    @test abs(sum(missing_rows[:,SyntheticPopulation.POPULATION_COLUMN]) - sum(filter(SyntheticPopulation.AGE_COLUMN => in([2,7,12,17]), marginal_ind_age_sex)[:,SyntheticPopulation.POPULATION_COLUMN])) / sum(missing_rows[:,SyntheticPopulation.POPULATION_COLUMN]) < 0.05

    #tests for forced_config
    forced_config_joint_distribution = generate_joint_distribution(
                                            marginal_ind_age_sex, 
                                            marginal_ind_sex_maritalstatus, 
                                            marginal_ind_income;
                                            config_file = "test/data/test_forced_config.json")
    forced_config1 = filter(SyntheticPopulation.AGE_COLUMN => ==(2), forced_config_joint_distribution)
    forced_config1 = filter(:income => !=(25394), forced_config1)
    @test sum(forced_config1[:,SyntheticPopulation.POPULATION_COLUMN]) == 0

    zero_row_ids = forced_config1[:, SyntheticPopulation.ID_COLUMN]
    @test sum(forced_config_joint_distribution[Not(zero_row_ids),SyntheticPopulation.POPULATION_COLUMN]) == sum(forced_config_joint_distribution[:,SyntheticPopulation.POPULATION_COLUMN])
            


    ##################################
    #tests for the main function######
    #assign_individuals_to_households#
    ##################################
    
    aggregated_households = generate_joint_distribution(marginal_hh_size)
    res = assign_individuals_to_households(joint_distribution, aggregated_households, return_unassigned = true)

    #check optional parameter
    @test length(res) == 2
    @test haskey(res[2], "unassigned_households")

    #check generated data
    @test res[1][8229,2] == 5
    @test nrow(res[1]) == sum(marginal_hh_size[:,SyntheticPopulation.POPULATION_COLUMN])

    

    ###################################
    #tests for the main function#######
    #generate_areas_dataframe_from_url#
    ###################################
    
    #download
    URL = "https://osm-boundaries.com/Download/Submit?apiKey=randomkey&db=osm20230403&osmIds=-2988903,-568660,-2988902,-2988901,-5505985,-2988900,-2988899,-2988898,-2988897,-5505984,-2988946,-2988896,-288600,-2988895,-2988933,-2988894&format=GeoJSON&srid=4326"
    @test_throws Any areas = generate_areas_dataframe_from_url(URL)
    
    #read from file
    areas = generate_areas_dataframe_from_file("test/data/file.geojson")
    areas.:population = SCALE .* 10000 .* [56.8, 313.2, 201.9, 345.1, 34.6, 184.0, 132.4, 45.7, 52.8, 39.3, 44.1, 131.3, 199.4, 226.9, 110.6, 70.9]
    @test typeof(areas.:geometry) == Vector{Union{GeoJSON.MultiPolygon{2, Float32}, GeoJSON.Polygon{2, Float32}}}
    @test "name_en" in names(areas)



    #############################
    #tests for the main function#
    #assign_areas_to_households##
    #############################
    
    dis_hh_areas = assign_areas_to_households!(res[1], aggregated_households, areas, return_unassigned = true)
    @test length(dis_hh_areas) == 2
    @test haskey(dis_hh_areas[2], "unassigned_areas")
    @test length(names(dis_hh_areas[1])) == 11
    @test sum(dis_hh_areas[1][:, :individuals_allocated]) > 0.5 * nrow(dis_hh_areas[1])

end