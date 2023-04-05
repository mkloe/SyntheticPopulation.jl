#Guo, J. Y., & Bhat, C. R. (2007). Population synthesis for microsimulating travel behavior. 
#Transportation Research Record, 2014(1), 92-101.


function get_no_ipf_df(no_ipf_merged_attributes::DataFrame, ipf_df1::DataFrame, ipf_df2::DataFrame)
    columns = names(no_ipf_merged_attributes)
    deleteat!(columns, findall(in(names(ipf_df2)), columns))
    no_of_unique_values = map(x -> length(unique(no_ipf_merged_attributes[:,x])), columns)
    column_index = only(findall(!=(1), no_of_unique_values))
    marginal_attr_values = combine(groupby(ipf_df1, [columns[column_index]]), :population => sum)
    rename!(marginal_attr_values, :population_sum => :population)
    
    return marginal_attr_values
end


function calculate_no_ipf_joint_distribution(dfs_for_ipf::Dict{String, DataFrame})
    no_ipf_merged_attributes = copy(dfs_for_ipf["no_ipf_merged_attributes"])
    no_ipf_df1 = dfs_for_ipf["no_ipf_df1"]
    no_ipf_df2 = dfs_for_ipf["no_ipf_df2"]
    no_ipf_joint_distribution = copy(no_ipf_merged_attributes)

    #size of target popoulation_size
    no_ipf_df1_nrow = nrow(no_ipf_df1)
    no_ipf_df2_nrow = nrow(no_ipf_df2)

    #add population size for no_ipf_joint_distribution
    if no_ipf_df1_nrow == 0 && no_ipf_df2_nrow == nrow(no_ipf_joint_distribution)
        sort!(no_ipf_df2, Not(:population))
        colnames = names(no_ipf_df2)
        sort!(no_ipf_joint_distribution, deleteat!(colnames, length(colnames)))
        no_ipf_joint_distribution.:population = no_ipf_df2.:population
    elseif no_ipf_df2_nrow == 0 && no_ipf_df1_nrow == nrow(no_ipf_joint_distribution)
        sort!(no_ipf_df1, Not(:population))
        colnames = names(no_ipf_df1)
        sort!(no_ipf_joint_distribution, deleteat!(colnames, length(colnames)))
        no_ipf_joint_distribution.:population = no_ipf_df1.:population
    else 
        if no_ipf_df1_nrow * no_ipf_df2_nrow == nrow(no_ipf_joint_distribution)
            nothing
        elseif no_ipf_df1_nrow == 0
            no_ipf_df1 = get_no_ipf_df(no_ipf_merged_attributes, ipf_df1, ipf_df2)
        elseif no_ipf_df2_nrow == 0
            no_ipf_df2 = get_no_ipf_df(no_ipf_merged_attributes, ipf_df2, ipf_df1)
        end
        
        no_ipf_merged_attributes.:compute_ipf = Int.(ones(nrow(no_ipf_merged_attributes)))
        no_ipf_dfs_for_ipf = Dict(
        "ipf_df1" => no_ipf_df1,
        "ipf_df2" => no_ipf_df2,
        "ipf_merged_attributes" => no_ipf_merged_attributes
        )
        no_ipf_joint_distribution = fit_ipf(no_ipf_dfs_for_ipf)
    end
    
    return no_ipf_joint_distribution
end


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
            X_prop = input_sample ./ sum(input_sample)
            Z = X_prop .* Array(af)
            population_size = min(sum(ipf_df1.:population), sum(ipf_df2.:population))
            Z = population_size .* Z
            Z = Int.(round.(Z))
        else
            X_prop = input_sample ./ sum(input_sample)
            Z = X_prop .* Array(af)
            population_size = min(sum(ipf_df1.:population), sum(ipf_df2.:population))
            Z = population_size .* Z
            Z = Int.(round.(Z))
        end
    end
    
    #generate output df from ipf
    joint_distribution = copy(ipf_merged_attributes)
    joint_distribution.:population = vec(Z)
    joint_distribution = joint_distribution[:, Not(:compute_ipf)]
    
    #generate output df not in ipf
    if haskey(dfs_for_ipf, "no_ipf_merged_attributes")
        no_ipf_joint_distribution = calculate_no_ipf_joint_distribution(dfs_for_ipf)  
        allowmissing!(joint_distribution)
        append!(joint_distribution, no_ipf_joint_distribution)
    end
    
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

    if haskey(dfs_for_ipf, "no_ipf_merged_attributes")
        no_ipf_merged_attributes = copy(dfs_for_ipf["no_ipf_merged_attributes"])
        no_ipf_df1 = copy(dfs_for_ipf["no_ipf_df1"])
        no_ipf_df2 = copy(dfs_for_ipf["no_ipf_df2"])

        no_ipf_df1_slice = filter(column => n -> n == unique_value, no_ipf_df1)
        no_ipf_df2_slice = filter(column => n -> n == unique_value, no_ipf_df2)
        no_ipf_merged_attributes_slice = filter(column => n -> n == unique_value, no_ipf_merged_attributes)

        dfs_for_ipf_slice["no_ipf_df1"] = no_ipf_df1_slice
        dfs_for_ipf_slice["no_ipf_df2"] = no_ipf_df2_slice
        dfs_for_ipf_slice["no_ipf_merged_attributes"] = no_ipf_merged_attributes_slice
    end

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


function generate_joint_distributions(marginal_attributes::DataFrame ...; config_file::Union{Nothing, String} = nothing)

    if length(marginal_attributes) == 1
        joint_distribution = marginal_attributes[1]
        joint_distribution.:population = Int.(round.(joint_distribution.:population))
        joint_distribution.:id = collect(1:nrow(joint_distribution))
    else
        joint_distribution = marginal_attributes[1]
        for i in 2:(length(marginal_attributes))
            dfs_for_ipf = merge_attributes(joint_distribution, marginal_attributes[i]; config_file = config_file)
            joint_distribution = compute_joint_distributions(dfs_for_ipf)
        end
    end

    #return proper dataframes
    joint_distribution.:id = collect(1:nrow(joint_distribution))
    aggregated_joint_distribution = joint_distribution[:, [:id, :population]]
    aggregated_joint_distribution.:population = convert.(Int, aggregated_joint_distribution[:, :population])
    joint_distribution = joint_distribution[:, Not(:population)]
    joint_distribution = joint_distribution[:, unique(vcat("id", names(joint_distribution)))] #change order


    return joint_distribution, aggregated_joint_distribution
end

##TO DO:
#- sort columns (from last to first) for input dataframes to ensure correct merging of data