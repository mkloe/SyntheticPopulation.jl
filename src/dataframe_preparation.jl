#=
Concept of setting cells in contingency tables to 0 based on:
Ponge, J., Enbergs, M., Schüngel, M., Hellingrath, B., Karch, A., & Ludwig, S. (2021, December). 
Generating synthetic populations based on german census data. In 2021 Winter Simulation Conference (WSC) (pp. 1-12). IEEE.
=#


"""
    read_json_file(filepath::String)

Auxilary function - it returns a parsed JSON file

Arguments:
- `filepath` - path to the JSON file.
"""
function read_json_file(filepath::String)
    json = read(filepath)
    dict = JSON3.read(json)
    
    return dict
end


"""
    get_config_elements(config_element::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})

Auxilary function - it returns a single parsed element of the config

Arguments:
- `config_element` - single element of the config file
"""
function get_config_elements(config_element::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
    if_dictionary = config_element["if"]
    then_dictionary = config_element["then"]
    if_column = only(keys(if_dictionary))
    if_values = only(values(if_dictionary))
    then_column = only(keys(then_dictionary))
    then_values = only(values(then_dictionary))
    
    return if_column, if_values, then_column, then_values
end


"""
    unique_attr_values(df::DataFrame)

Auxilary function - it returns an array of tuples. Each tuple is a column name and unique values in this column.

Arguments:
- `df` - data frame, for which the array of tuples with column names and values are generated
"""
function unique_attr_values(df::DataFrame)
    df = select(df, Not(POPULATION_COLUMN))
    df_names = names(df)
    res = Tuple{String, Vector}[]
    for column in df_names
        unique_values = unique(df[:, Symbol(column)])
        tuple = (column, unique_values)
        push!(res, tuple)
    end
    
    return res
end


"""
    get_dictionary_dfs_for_ipf(df1::DataFrame, df2::DataFrame)

Auxilary function - it returns a dictionary with data frames that are used for generation of joint distribution of attributes.

Arguments:
- `df1` - first data frame that is to be merged
- `df2` - second data frame that is to be merged
"""
function get_dictionary_dfs_for_ipf(df1::DataFrame, df2::DataFrame)
    df1[:,POPULATION_COLUMN] = Int.(round.(df1[:,POPULATION_COLUMN]))
    df2[:,POPULATION_COLUMN] = Int.(round.(df2[:,POPULATION_COLUMN]))
    df1_copy = copy(df1)
    df2_copy = copy(df2)

    df1_unique_attr_values = unique_attr_values(df1_copy)
    df2_unique_attr_values = unique_attr_values(df2_copy)
    
    #deleting intersecting columns from combinations
    intersecting_columns = names(df2_copy)[findall(in(names(df1_copy)), names(df2_copy))]
    for element in df2_unique_attr_values
        column, values = element
        if column in intersecting_columns
            deleteat!(df1_unique_attr_values, findall(x -> String(first(x)) == String(column), df1_unique_attr_values))
        end
    end

    #create product dataframe 
    df1_possible_values = map(last, df1_unique_attr_values)
    df2_possible_values = map(last, df2_unique_attr_values)
    possible_values = vcat(df1_possible_values, df2_possible_values)
    values_combinations = collect(Iterators.product(possible_values...))
    merged_attributes = DataFrame(vec(values_combinations))
    df1_columns = map(first, df1_unique_attr_values)
    df2_columns = map(first, df2_unique_attr_values)
    column_names = Symbol.(vcat(df1_columns, df2_columns))
    rename!(merged_attributes, column_names)

    dfs_for_ipf = Dict(
        "ipf_merged_attributes" => merged_attributes,
        "ipf_df1" => df1_copy,
        "ipf_df2" => df2_copy)

    return dfs_for_ipf
end


"""
    indices_for_compute_ipf(dictionary::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}, merged_attributes::DataFrame)

Auxilary function - it returns a vector of indices of rows of the data frame that meet criteria specified by the arguments

Arguments:
- `dictionary` - parsed element of array from config JSON file that specifies FORCED config
- `merged_attributes` - data frame, whose row indices are extracted
"""
function indices_for_compute_ipf(dictionary::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}, merged_attributes::DataFrame)
    temp_df = copy(merged_attributes)
    if_column, if_values, then_column, then_values = get_config_elements(dictionary)
    attribute_names = names(merged_attributes)
    
    if String(if_column) in attribute_names && String(then_column) in attribute_names
        #find indices for "if"s
        if typeof(if_values[1]) == String
            temp_df = filter(Symbol(if_column) => x -> in(if_values, x), temp_df)
        elseif typeof(if_values[1]) == Int
            temp_df = filter(Symbol(if_column) => x -> x in if_values, temp_df)
        end

        #find_indices for no-"then's"
        if typeof(then_values[1]) == String
            temp_df = filter(Symbol(then_column) => x -> !in(then_values, x), temp_df)
        elseif typeof(then_values[1]) == Int
            temp_df = filter(Symbol(then_column) => x -> !(x in then_values), temp_df)
        end
    end
    
    return temp_df[:,ID_COLUMN]
end


"""
    get_zero_indices(config_file::String, merged_attributes::DataFrame)

Auxilary function - it returns an array of all indices of rows of a data frame, that are specified by a config JSON file

Arguments:
- `config_file` - path of the config JSON file
- `merged_attributes` - data frame, whose row indices are extracted
"""
function get_zero_indices(config_file::String, merged_attributes::DataFrame)
    config = read_json_file(config_file)
    forced_config = config["forced_config"] 
    zero_indices = Int[]
    if forced_config != "missing"
        for dictionary in forced_config
            indices = indices_for_compute_ipf(dictionary, merged_attributes)
            append!(zero_indices, indices)
            unique!(zero_indices)
        end
    end
    return zero_indices
end


"""
    merge_attributes(df1::DataFrame, df2::DataFrame; config_file::Union{String, Nothing})

Auxilary function - it returns all the data frames that are needed in order to generate a joint distribution of attributes of two data frames

Arguments:
- `df1` - the first data frame, that is to be merged
- `df2` - the second data frame, that is to be merged
- `config_file` - optional argument; path to the JSON file that specifies merging config
"""
function merge_attributes(df1::DataFrame, df2::DataFrame; config_file::Union{String, Nothing})
    #merge the dfs with merginal attributes into 1 dataframe
    dfs_for_ipf = get_dictionary_dfs_for_ipf(df1, df2)

    #add :compute_ipf column. 0 -> do not compute ipf; 1 -> compute ipf
    merged_attributes = copy(dfs_for_ipf["ipf_merged_attributes"])
    if config_file === nothing
        merged_attributes.:compute_ipf = Int.(ones(nrow(merged_attributes)))
    else
        #add id column
        merged_attributes[:,ID_COLUMN] = collect(1:nrow(merged_attributes))
        #indices set to 0
        zero_indices = get_zero_indices(config_file, merged_attributes)
        #create compute_ipf column
        merged_attributes.:compute_ipf = [(i in zero_indices) ? 0 : 1 for i in 1:nrow(merged_attributes)]
        #drop id column
        merged_attributes = merged_attributes[:, Not(ID_COLUMN)]
    end
    
    dfs_for_ipf["ipf_merged_attributes"] = merged_attributes

    return dfs_for_ipf
end