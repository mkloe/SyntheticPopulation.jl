#Guo, J. Y., & Bhat, C. R. (2007). Population synthesis for microsimulating travel behavior. 
#Transportation Research Record, 2014(1), 92-101.


function fit_ipf(dfs_for_ipf::Dict{String, DataFrame})
    ipf_merged_attributes = dfs_for_ipf["ipf_merged_attributes"]
    ipf_df1 = dfs_for_ipf["ipf_df1"]
    ipf_df2 = dfs_for_ipf["ipf_df2"]

    #initialize sample matrix as seed for IPF
    input_sample = reshape(ipf_merged_attributes.:compute_ipf, (nrow(ipf_df1), nrow(ipf_df2)))

    #initiate input marginal distributions for IPF
    input_marginals = vcat([ipf_df1.:population], [ipf_df2.:population])
    population_size_difference = abs(sum(ipf_df1.:population) - sum(ipf_df2.:population))

    #fit using the iterative proportional fitting
    if population_size_difference == 0
        fac = ipf(input_sample, input_marginals)
        Z = Array(fac) .* input_sample
        Z = Int.(round.(Z))
    else 
        af = ipf(input_sample, ArrayMargins(input_marginals))
        if isnan(Array(af)[1]) #error handling
            input_sample = input_sample .+ 0.00000000000000000001 # to avoid dividing by zero
            af = ipf(input_sample, input_marginals)
        end
        X_prop = input_sample ./ sum(input_sample)
        Z = X_prop .* Array(af)
        population_size = max(sum(ipf_df1.:population), sum(ipf_df2.:population))
        Z = population_size .* Z
        Z = Int.(round.(Z))
    end
    
    #generate output df from ipf
    joint_distribution = copy(ipf_merged_attributes)
    joint_distribution.:population = vec(Z)
    joint_distribution = joint_distribution[:, Not(:compute_ipf)]
    
    return joint_distribution
end


function get_dfs_for_ipf_slice(dfs_for_ipf::Dict{String, DataFrame}, unique_value::Any, column::Union{String, Symbol})    
    dfs_for_ipf_slice = Dict{String, DataFrame}()
    ipf_merged_attributes = copy(dfs_for_ipf["ipf_merged_attributes"])
    ipf_df1 = copy(dfs_for_ipf["ipf_df1"])
    ipf_df2 = copy(dfs_for_ipf["ipf_df2"])
    
    ipf_df1_slice = filter(column => n -> n == unique_value, ipf_df1)
    ipf_df2_slice = filter(column => n -> n == unique_value, ipf_df2)
    ipf_merged_attributes_slice = filter(column => n -> n == unique_value, ipf_merged_attributes)

    dfs_for_ipf_slice["ipf_df1"] = ipf_df1_slice
    dfs_for_ipf_slice["ipf_df2"] = ipf_df2_slice
    dfs_for_ipf_slice["ipf_merged_attributes"] = ipf_merged_attributes_slice

    return dfs_for_ipf_slice
end 


function compute_joint_distributions(dfs_for_ipf::Dict{String, DataFrame}; shared_columns::Vector{String} = String[])
    ipf_df1 = copy(dfs_for_ipf["ipf_df1"])
    ipf_df2 = copy(dfs_for_ipf["ipf_df2"])
    intersecting_columns = names(ipf_df2)[findall(in(names(ipf_df1)), names(ipf_df2))]
    deleteat!(intersecting_columns, findall(x -> x == "population", intersecting_columns))
    intersecting_columns = setdiff(intersecting_columns, shared_columns)
    
    if length(intersecting_columns) == 0
        return fit_ipf(dfs_for_ipf)

    else 
        attributes = copy(shared_columns)
        push!(attributes, intersecting_columns[1])
        unique_values_of_attribute = unique(ipf_df1[:, intersecting_columns[1]])
        
        if length(unique_values_of_attribute) == 1
            return fit_ipf(dfs_for_ipf)
        end

        df_list = DataFrame[]
        for unique_value in unique_values_of_attribute
            dfs_for_ipf_slice = copy(dfs_for_ipf)
            dfs_for_ipf_slice = get_dfs_for_ipf_slice(dfs_for_ipf_slice, unique_value, intersecting_columns[1])        
            joint_distribution = compute_joint_distributions(dfs_for_ipf_slice; shared_columns = attributes)
    
            push!(df_list, joint_distribution)
        end
        
        return reduce(vcat, df_list)
    end
end


function apply_missing_config(joint_distribution::DataFrame, missing_config::JSON3.Array{JSON3.Object, Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}})
    joint_distribution.:id = collect(1:nrow(joint_distribution))
    attribute_names = names(joint_distribution)    
    for dictionary in missing_config
        if_column, if_values, then_column, then_values = get_config_elements(dictionary)
        
        if String(if_column) in attribute_names && String(then_column) in attribute_names
            temp_df = copy(joint_distribution)
            allowmissing!(joint_distribution)
            
            #find indices for "if"s
            if typeof(if_values[1]) == String
                temp_df = filter(Symbol(if_column) => x -> in(if_values, x), temp_df)
            elseif typeof(if_values[1]) == Int
                temp_df = filter(Symbol(if_column) => x -> x in if_values, temp_df)
            end
            joint_distribution[temp_df.:id, then_column] .= missing
        end
    end

    #split-apply-combine procedure for the columns including missing
    deleteat!(attribute_names, findall(x -> String(x) in ["population", "id"], attribute_names))
    aggregated_joint_distribution = groupby(joint_distribution, attribute_names)
    aggregated_joint_distribution = combine(aggregated_joint_distribution, :population => sum)
    
    #return proper dataframes
    aggregated_joint_distribution.:population = convert.(Int, aggregated_joint_distribution[:, :population_sum])
    aggregated_joint_distribution = aggregated_joint_distribution[:, Not(:population_sum)]
    
    return aggregated_joint_distribution
end


function generate_joint_distributions(marginal_attributes::DataFrame ...; config_file::Union{Nothing, String} = nothing)

    for dataframe in marginal_attributes
        sort!(dataframe, reverse(deleteat!(names(dataframe), findall(x -> String(x) == "population", names(dataframe)))))
    end

    if length(marginal_attributes) == 1
        joint_distribution = marginal_attributes[1]
        joint_distribution.:population = Int.(round.(joint_distribution.:population))
        joint_distribution.:id = collect(1:nrow(joint_distribution))
    else
        joint_distribution = marginal_attributes[1]
        for i in 2:(length(marginal_attributes))
            dfs_for_ipf = merge_attributes(joint_distribution, marginal_attributes[i]; config_file = config_file)
            joint_distribution = compute_joint_distributions(dfs_for_ipf)
            sort!(joint_distribution, reverse(deleteat!(names(joint_distribution), findall(x -> String(x) == "population", names(joint_distribution)))))
        end
    end
    
    #set the attribute values from MISSING config file to missing
    if config_file !== nothing
        config_file = read_json_file(config_file)
        missing_config = config_file["missing_config"]
        if missing_config != "missing"
            aggregated_joint_distribution = apply_missing_config(joint_distribution, missing_config)
        else
            aggregated_joint_distribution = joint_distribution
        end
    else 
        aggregated_joint_distribution = joint_distribution
    end
    
    #add ID column for
    aggregated_joint_distribution.:id = collect(1:nrow(aggregated_joint_distribution))
    aggregated_joint_distribution = select(aggregated_joint_distribution, :id, :)

    return aggregated_joint_distribution
end