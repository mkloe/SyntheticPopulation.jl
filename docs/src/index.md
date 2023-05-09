# SyntheticPopulation

```@meta
CurrentModule = SyntheticPopulation
DocTestSetup = quote
    using SyntheticPopulation
end
```

Downloading and parsing data about boundaries of areas
-----------------
```@docs
read_geojson_file(path::String)
download_osm_boundaries(url::String, target_filepath::String = pwd())
generate_areas_dataframe_from_url(url::String, target_filepath::String = pwd())
generate_areas_dataframe_from_file(filepath::String)
```


Preparing two data frames for merging
-----------------
```@docs
read_json_file(filepath::String)
get_config_elements(config_element::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
unique_attr_values(df::DataFrame)
get_dictionary_dfs_for_ipf(df1::DataFrame, df2::DataFrame)
indices_for_compute_ipf(dictionary::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}, merged_attributes::DataFrame)
get_zero_indices(config_file::String, merged_attributes::DataFrame)
merge_attributes(df1::DataFrame, df2::DataFrame; config_file::Union{String, Nothing})
```


Merging multiple data frames to generate a joint distribution
-----------------
```@docs
fit_ipf(dfs_for_ipf::Dict{String, DataFrame})
get_dfs_for_ipf_slice(dfs_for_ipf::Dict{String, DataFrame}, unique_value::Any, column::Union{String, Symbol})
compute_joint_distributions(dfs_for_ipf::Dict{String, DataFrame}; shared_columns::Vector{String} = String[])
apply_missing_config(joint_distribution::DataFrame, missing_config::JSON3.Array{JSON3.Object, Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
generate_joint_distribution(marginal_distributions::DataFrame ...; config_file::Union{Nothing, String} = nothing)
```


Allocation of individuals into households
-----------------
```@docs
show_statistics(aggregated_individuals::DataFrame, aggregated_households::DataFrame, total_individual_population::Int, total_household_population::Int)
update_dfs!(aggregated_idividuals::DataFrame, disaggregated_households::DataFrame, available_individuals::DataFrame, individual_index::Int, household_id::Int, individual_type::String)
select_household_head!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_size::Int, household_id::Int) 
select_partner!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_head_sex::Union{String, Char}, hh_head_age::Int, household_id::Int)
select_child!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, child_number::Int, hh_head_age::Int, partner_age::Int, household_id::Int)
allocate_household_members!(disaggregated_households::DataFrame, aggregated_individuals::DataFrame, hh_size::Int)
spread(vals,cnts)
expand_df_from_row_counts(dataframe::DataFrame)
assign_individuals_to_households(aggregated_individuals::DataFrame, aggregated_households::DataFrame; return_unassigned::Bool = false)
```

Allocation of coordinates to households
-----------------
```@docs
get_coordinates(geometry)
find_point_in_polygon(area::DataFrameRow)
assign_areas_to_households!(disaggregated_households::DataFrame, aggregated_households::DataFrame, aggregated_areas::DataFrame; return_unassigned::Bool = false)
```

