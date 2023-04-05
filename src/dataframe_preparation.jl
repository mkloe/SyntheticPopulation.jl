#Concept of setting cells in contingency tables to 0 based on:
#Ponge, J., Enbergs, M., Schüngel, M., Hellingrath, B., Karch, A., & Ludwig, S. (2021, December). 
#Generating synthetic populations based on german census data. In 2021 Winter Simulation Conference (WSC) (pp. 1-12). IEEE.


function read_json_file(filepath::String)
    json = read(filepath)
    dict = JSON3.read(json)
    
    return dict
end


function no_ipf_get_if_then_columns(no_ipf_element::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
    if_dictionary = no_ipf_element["if"]
    then_dictionary = no_ipf_element["then"]
    if_column = only(keys(if_dictionary))
    if_values = only(values(if_dictionary))
    then_column = only(keys(then_dictionary))
    then_values = only(values(then_dictionary))
    
    return if_column, if_values, then_column, then_values
end


function filter_out_missing_attr_combinations!(dataframe::DataFrame, no_ipf::JSON3.Array{JSON3.Object, Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
    for element in no_ipf
        if_column, if_values, then_column, then_values = no_ipf_get_if_then_columns(element)
        if String(then_column) in names(dataframe)
            dataframe = filter(then_column => x -> typeof(x) != Missing, dataframe)
        end
    end
    return dataframe
end


function create_no_ipf_df(no_ipf_df_list::Vector{DataFrame}, ipf_df::DataFrame)
    if length(no_ipf_df_list) !=0
        no_ipf_df = reduce(vcat, no_ipf_df_list)
        unique_rows_df = findall(==(false), nonunique(no_ipf_df))
        no_ipf_df = no_ipf_df[unique_rows_df, :]
    else
        no_ipf_df = DataFrame([name => [] for name in names(ipf_df)])
    end
    
    return no_ipf_df
end


function create_no_ipf_merged_attributes(no_ipf_merged_attributes_list::Vector{DataFrame}, then_columns::Vector{String})
    no_ipf_merged_attributes = reduce(vcat, no_ipf_merged_attributes_list)
    allowmissing!(no_ipf_merged_attributes)
    for column in then_columns
        if column in names(no_ipf_merged_attributes)
            no_ipf_merged_attributes[:, column] .= missing
        end
    end
    unique_rows = findall(==(false), nonunique(no_ipf_merged_attributes))
    no_ipf_merged_attributes = no_ipf_merged_attributes[unique_rows, :]
    
    return no_ipf_merged_attributes
end


function prepare_dfs_for_ipf(df1::DataFrame, df2::DataFrame, merged_attributes::DataFrame, no_ipf::JSON3.Array{JSON3.Object, Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
    no_ipf_merged_attributes = copy(merged_attributes)
    ipf_merged_attributes = copy(merged_attributes)
    ipf_df1 = copy(df1)
    ipf_df2 = copy(df2)
    
    #initialize variables
    no_ipf_merged_attributes_list = DataFrame[]
    then_columns = String[]
    no_ipf_df1_list = DataFrame[]
    no_ipf_df2_list = DataFrame[]

    for element in no_ipf
        #get needed values from the dictionary
        if_column, if_values, then_column, then_values = no_ipf_get_if_then_columns(element)
        push!(then_columns, String(then_column))
        
        if String(if_column) in names(merged_attributes) && String(then_column) in names(merged_attributes)
            #filter the dataframe fed into ipf
            ipf_merged_attributes = filter(if_column => x -> !in(x, if_values), ipf_merged_attributes)
            #create the dataframe slice which will not be fed into ipf
            no_ipf_merged_attributes = filter(if_column => x -> in(x, if_values), merged_attributes)
            push!(no_ipf_merged_attributes_list, no_ipf_merged_attributes)

            #create ipf_df and no_ipf_df
            if String(if_column) in names(df1)
                ipf_df1 = filter(if_column => x -> !in(x, if_values), ipf_df1)
                no_ipf_df1 = filter(if_column => x -> in(x, if_values), df1)
                push!(no_ipf_df1_list, no_ipf_df1)
            elseif String(if_column) in names(df2)
                no_ipf_df2 = filter(if_column => x -> in(x, if_values), df2)
                ipf_df2 = filter(if_column => x -> !in(x, if_values), ipf_df2)
                push!(no_ipf_df2_list, no_ipf_df2)
            end
        end
    end

    #create no_ipf_merged_attributes
    if length(no_ipf_merged_attributes_list) == 0
        return Dict("status" => 1)
    else
        no_ipf_merged_attributes = create_no_ipf_merged_attributes(no_ipf_merged_attributes_list, then_columns)
    end

    #create no_ipf_df1 and no_ipf_df2
    no_ipf_df1 = create_no_ipf_df(no_ipf_df1_list, ipf_df1)
    no_ipf_df2 = create_no_ipf_df(no_ipf_df2_list, ipf_df2)
    
    dfs_for_ipf = Dict(
        "ipf_merged_attributes" => ipf_merged_attributes,
        "no_ipf_merged_attributes" => no_ipf_merged_attributes,
        "ipf_df1" => ipf_df1,
        "no_ipf_df1" => no_ipf_df1,
        "ipf_df2" => ipf_df2,
        "no_ipf_df2" => no_ipf_df2)

    return Dict("status" => 0, "result" => dfs_for_ipf)
end


function unique_attr_values(df::DataFrame)
    df = select(df, Not(:population))
    df_names = names(df)
    res = Tuple{String, Vector}[]
    for column in df_names
        unique_values = unique(df[:, Symbol(column)])
        tuple = (column, unique_values)
        push!(res, tuple)
    end
    
    return res
end


function merge_attributes_with_config(df1::DataFrame, df2::DataFrame, config_file::Union{String, Nothing})
    df1.:population = Int.(round.(df1.population))
    df2.:population = Int.(round.(df2.population))
    df1_no_missing = copy(df1)
    df2_no_missing = copy(df2)

    df1_unique_attr_values = unique_attr_values(df1_no_missing)
    df2_unique_attr_values = unique_attr_values(df2_no_missing)
    
    #deleting intersecting columns from combinations
    intersecting_columns = names(df2_no_missing)[findall(in(names(df1_no_missing)), names(df2_no_missing))]
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
        "ipf_df1" => df1_no_missing,
        "ipf_df2" => df2_no_missing)

    if config_file !== nothing
        #filter out incorrect attribute combinations (missing)
        config = read_json_file(config_file)
        no_ipf = config["no_ipf"]
        if no_ipf != "missing"
            merged_attributes = filter_out_missing_attr_combinations!(merged_attributes, no_ipf)
            
            #divide merged_attributes into rows for ipf and not for ipf
            response = prepare_dfs_for_ipf(df1, df2, merged_attributes, no_ipf)
            if response["status"] == 0
                dfs_for_ipf = response["result"]
            elseif response["status"] == 1
                nothing
            end
        end
    end

    return dfs_for_ipf
end


function indices_for_compute_ipf(dictionary::JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}, merged_attributes::DataFrame)
    temp_df = copy(merged_attributes)
    if_column, if_values, then_column, then_values = no_ipf_get_if_then_columns(dictionary)
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
    
    return temp_df.:id
end


function get_zero_indices(config_file::String, merged_attributes::DataFrame)
    config = read_json_file(config_file)
    ipf_forced_attributes = config["ipf_forced_attributes"] 
    zero_indices = Int[]
    if ipf_forced_attributes != "missing"
        for dictionary in ipf_forced_attributes
            indices = indices_for_compute_ipf(dictionary, merged_attributes)
            append!(zero_indices, indices)
            unique!(zero_indices)
        end
    end
    return zero_indices
end


function merge_attributes(df1::DataFrame, df2::DataFrame; config_file::Union{String, Nothing})
    #merge the dfs with merginal attributes into 1 dataframe
    dfs_for_ipf = merge_attributes_with_config(df1, df2, config_file)

    #add :compute_ipf column. 0 -> do not compute ipf; 1 -> compute ipf
    merged_attributes = copy(dfs_for_ipf["ipf_merged_attributes"])
    if config_file === nothing
        merged_attributes.:compute_ipf = Int.(ones(nrow(merged_attributes)))
    else
        #add id column
        merged_attributes.:id = collect(1:nrow(merged_attributes))
        #indices set to 0
        zero_indices = get_zero_indices(config_file, merged_attributes)
        #create compute_ipf column
        merged_attributes.:compute_ipf = [(i in zero_indices) ? 0 : 1 for i in 1:nrow(merged_attributes)]
        #drop id column
        merged_attributes = merged_attributes[:, Not(:id)]
    end
    
    dfs_for_ipf["ipf_merged_attributes"] = merged_attributes

    return dfs_for_ipf
end