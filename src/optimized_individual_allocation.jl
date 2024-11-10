using JuMP
using GLPK
using HiGHS
using Cbc
using DataFrames

using ShiftedArrays
using LinearAlgebra


function findrow(cumulative_population, individual_id)
    for i = 1:length(cumulative_population)
        if individual_id <= cumulative_population[i]
            return i
        end
    end
    return 0  # If individual_id is below the last index, return the last index
end



"""
    add_indices_range_to_indiv(aggregated_individuals::DataFrame)

Adds individual index ranges to the `aggregated_individuals` DataFrame based on population counts, calculating a range of individual indices for each row and storing it in a new column, `indiv_indices`.

# Arguments
- `aggregated_individuals::DataFrame`: A DataFrame containing individual data with at least a `population` column that represents the count of individuals for each row.

# Returns
- `DataFrame`: The modified `aggregated_individuals` DataFrame with a new column, `indiv_indices`, representing the individual index range for each row.
"""
function add_indices_range_to_indiv(aggregated_individuals::DataFrame)
    aggregated_individuals = copy(aggregated_individuals)
    aggregated_individuals[!,"indiv_range_to"] .= cumsum(aggregated_individuals[!, POPULATION_COLUMN])
    aggregated_individuals[!, "indiv_range_from"] = replace(ShiftedArrays.lag(aggregated_individuals[!, "indiv_range_to"], 1), missing => 0)
    aggregated_individuals[!, "indiv_indices"] = map(row -> row.indiv_range_from+1:row.indiv_range_to, eachrow(aggregated_individuals))
    rename!(aggregated_individuals,:indiv_range_to => :cum_population)
    aggregated_individuals = select(aggregated_individuals, Not([:indiv_range_from]))
    return aggregated_individuals
end



"""
    add_individual_flags(aggregated_individuals::DataFrame)

Adds useful categorical flags to the `aggregated_individuals` DataFrame, determining whether each individual is an adult, a potential parent, or a potential child based on age and marital status.

# Arguments
- `aggregated_individuals::DataFrame`: A DataFrame containing individual data.

# Returns
- `DataFrame`: The modified `aggregated_individuals` DataFrame.
"""
function add_individual_flags(aggregated_individuals::DataFrame)
    aggregated_individuals = copy(aggregated_individuals)
    aggregated_individuals[!, "is_adult"] = aggregated_individuals[!, AGE_COLUMN] .>= MINIMUM_ADULT_AGE
    aggregated_individuals[!, "is_potential_parent"] = (aggregated_individuals[!, "is_adult"] .== true) .&& (aggregated_individuals[!, MARITALSTATUS_COLUMN] .== AVAILABLE_FOR_MARRIAGE)
    # Tak warunek wyglada obecnie w kodzie. Divorced osoba w wieku 60 lat nadaje sie do bycia child
    aggregated_individuals[!, "is_potential_child"] = (aggregated_individuals[!, "is_adult"] .== false) .|| (aggregated_individuals[!, MARITALSTATUS_COLUMN] .!= AVAILABLE_FOR_MARRIAGE)
    return aggregated_individuals
end



"""
    add_indices_range_to_hh(aggregated_households::DataFrame)

Adds household index ranges to the `aggregated_households` DataFrame based on population counts, creating a new column `hh_indices` that assigns a unique range of indices to each household based on cumulative population values.

# Arguments
- `aggregated_households::DataFrame`: A DataFrame containing household data with at least a `population` column representing the number of individuals in each household.

# Returns
- `DataFrame`: The modified `aggregated_households` DataFrame with the new column `hh_indices`, which lists index ranges for each household.
"""
function add_indices_range_to_hh(aggregated_households::DataFrame)
    aggregated_households = copy(aggregated_households)
    aggregated_households[!,"hh_range_to"] .= cumsum(aggregated_households[!, POPULATION_COLUMN])
    aggregated_households[!, "hh_range_from"] = replace(ShiftedArrays.lag(aggregated_households[!, "hh_range_to"], 1), missing => 0)
    aggregated_households[!, "hh_indices"] = map(row -> row.hh_range_from+1:row.hh_range_to, eachrow(aggregated_households))
    rename!(aggregated_households,:hh_range_to => :cum_population)
    aggregated_households = select(aggregated_households, Not([:hh_range_from]))
    return aggregated_households
end

"""
    prep_group_indices_for_indv_constraints(aggregated_individuals::DataFrame)

Processes individual-level constraints from an aggregated DataFrame by extracting indices based on demographic and relational characteristics such as age, marital status, and parental status.

# Arguments
- `aggregated_individuals::DataFrame`: A DataFrame containing individual data.

# Returns
A tuple containing:
- `adult_indices::Vector{Int}`: Vector of indices for adults.
- `married_male_indices::Vector{Int}`: Vector of indices for married males.
- `married_female_indices::Vector{Int}`: Vector of indices for married females.
- `parent_indices::Vector{Int}`: Vector of indices for potential parents.
- `child_indices::Vector{Int}`: Vector of indices for potential children.
- `individuals_age_vect::Vector{Int}``: Vector of individuals age.
"""
function prep_group_indices_for_indv_constraints(aggregated_individuals::DataFrame)
    # adult_indices
    filtered_df = filter(row -> row.is_adult == true, aggregated_individuals)
    adult_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "indiv_indices"]])

    # married_male_indices
    filtered_df = filter(row ->  coalesce(row[SEX_COLUMN] == 'M', false) && coalesce(row[MARITALSTATUS_COLUMN] == AVAILABLE_FOR_MARRIAGE, false) && row.is_adult == true, aggregated_individuals)
    married_male_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "indiv_indices"]])

    # married_female_indice
    filtered_df = filter(row ->  coalesce(row[SEX_COLUMN] == 'F', false) && coalesce(row[MARITALSTATUS_COLUMN] == AVAILABLE_FOR_MARRIAGE, false) && row.is_adult == true, aggregated_individuals)
    married_female_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "indiv_indices"]])

    # parent_indices
    filtered_df = filter(row -> row.is_potential_parent == true, aggregated_individuals)
    parent_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "indiv_indices"]])


    # child_indices
    filtered_df = filter(row -> row.is_potential_child == true, aggregated_individuals)
    child_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "indiv_indices"]])

    # age vector
    individuals_age_vect = reduce(vcat, [fill(size, length(r)) for (size, r) in zip(aggregated_individuals[!, "age"], aggregated_individuals[!, "indiv_indices"])])

    return adult_indices, married_male_indices, married_female_indices, parent_indices, child_indices, individuals_age_vect
end


"""
    prep_group_indices_for_hh_constraints(aggregated_households::DataFrame)

Prepares and organizes household indices by household size, based on a provided DataFrame of aggregated household data.

# Arguments
- `aggregated_households::DataFrame`: A DataFrame containing household data.

# Returns
A tuple containing:
- `hh_size1_indices::Vector{Int}`: Vector of indices for households of size 1.
- `hh_size2_indices::Vector{Int}`: Vector of indices for households of size 2.
- `hh_size3plus_indices::Vector{Int}`: Vector of indices for households of size 3 or more.
- `hh_size3plus_capacity::Vector{Int}`: Vector indicating the household sizes for each index in `hh_size3plus_indices`.
"""
function prep_group_indices_for_hh_constraints(aggregated_households::DataFrame)

    # hh_size1_indices
    filtered_df = filter(row -> row[HOUSEHOLD_SIZE_COLUMN] == 1, aggregated_households)
    hh_size1_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "hh_indices"]])

    # hh_size2_indices
    filtered_df = filter(row -> row[HOUSEHOLD_SIZE_COLUMN] == 2, aggregated_households)
    hh_size2_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "hh_indices"]])

    # hh_size3plus_indices
    filtered_df = filter(row -> row[HOUSEHOLD_SIZE_COLUMN] >= 3, aggregated_households)
    hh_size3plus_indices = reduce(vcat, [collect(r) for r in filtered_df[!, "hh_indices"]])
    hh_size3plus_capacity = reduce(vcat, [fill(size, length(r)) for (size, r) in zip(filtered_df[!, HOUSEHOLD_SIZE_COLUMN], filtered_df[!, "hh_indices"])])

    return hh_size1_indices, hh_size2_indices, hh_size3plus_indices, hh_size3plus_capacity
end


"""
    define_and_run_optimization(aggregated_individuals::DataFrame,
                                 aggregated_households::DataFrame,
                                 hh_size1_indices::Vector{Int},
                                 hh_size2_indices::Vector{Int},
                                 hh_size3plus_indices::Vector{Int},
                                 hh_size3plus_capacity::Vector{Int},
                                 adult_indices::Vector{Int},
                                 married_male_indices::Vector{Int},
                                 married_female_indices::Vector{Int},
                                 parent_indices::Vector{Int},
                                 child_indices::Vector{Int})

Run an optimization linear programming model to allocate individuals to households based on specific constraints related to household size and family structure.

# Arguments
- `aggregated_individuals::DataFrame`: A DataFrame containing the population data for individuals, including identifiers and demographic details.
- `aggregated_households::DataFrame`: A DataFrame containing the population data for households, including identifiers and household size information.
- `hh_size1_indices::Vector{Int}`: Indices of households that can accommodate 1 individual.
- `hh_size2_indices::Vector{Int}`: Indices of households that can accommodate 2 individuals.
- `hh_size3plus_indices::Vector{Int}`: Indices of households that can accommodate 3 or more individuals.
- `hh_size3plus_capacity::Vector{Int}`: A vector containing the capacity of households that can accommodate 3 or more individuals.
- `adult_indices::Vector{Int}`: Indices of individuals classified as adults.
- `married_male_indices::Vector{Int}`: Indices of married male individuals.
- `married_female_indices::Vector{Int}`: Indices of married female individuals.
- `parent_indices::Vector{Int}`: Indices of individuals classified as parents.
- `child_indices::Vector{Int}`: Indices of individuals classified as children.

# Returns
- `Matrix{Float64}`: A vector containing the allocation results, where each element indicates the household assigned to each individual. If an individual is not allocated to any household, the entry will be `missing`.
"""
function define_and_run_optimization(aggregated_individuals::DataFrame
                                    , aggregated_households::DataFrame

                                    , hh_size1_indices::Vector{Int}
                                    , hh_size2_indices::Vector{Int}
                                    , hh_size3plus_indices::Vector{Int}
                                    , hh_size3plus_capacity::Vector{Int}

                                    , adult_indices::Vector{Int}
                                    , married_male_indices::Vector{Int}
                                    , married_female_indices::Vector{Int}
                                    , parent_indices::Vector{Int}
                                    , child_indices::Vector{Int}
                                    , age_vector::Vector{Int}
                                    )

    # Create a new optimization mode
    model = Model(Cbc.Optimizer)

    # Define decision variables: a binary allocation matrix where
    # allocation[i, j] indicates whether individual i is assigned to household j
    @variable(model, allocation[1:sum(aggregated_individuals[!,POPULATION_COLUMN]), 1:sum(aggregated_households[!,POPULATION_COLUMN])], Bin, start = 0)
    @variable(model, penalty[1:sum(aggregated_households[!,POPULATION_COLUMN])], lower_bound=0, start = 0)

    # Define the objective function: maximize the total number of assigned individuals
    @objective(model, Max, sum(allocation) - sum(penalty) ) #)
    
    # Add constraints to the model

    # Each individual can only be assigned to one household
    @constraint(model, [i=1:size(allocation, 1)], sum(allocation[i, j] for j in 1:size(allocation, 2)) <= 1)

    # Constraints for households of size 1
    # There can be 0 or 1 adult
    @constraint(model, [hh_id=hh_size1_indices], sum(allocation[indv_id, hh_id] for indv_id in adult_indices) <= 1) 
    # There can be 0 children
    @constraint(model, [hh_id=hh_size1_indices], sum(allocation[indv_id, hh_id] for indv_id in child_indices) == 0) 
    
    # Constraints for households of size 2
    # There must be at least 1 adult
    @constraint(model, [hh_id=hh_size2_indices], sum(allocation[indv_id, hh_id] for indv_id in adult_indices) >= 1) 
    # There must be exactly one married adult male
    @constraint(model, [hh_id=hh_size2_indices], sum(allocation[indv_id, hh_id] for indv_id in married_male_indices) == 1) 
    # There must be exactly one married adult female
    @constraint(model, [hh_id=hh_size2_indices], sum(allocation[indv_id, hh_id] for indv_id in married_female_indices) == 1) 
    # There must be 0 children
    @constraint(model, [hh_id=hh_size2_indices], sum(allocation[indv_id, hh_id] for indv_id in child_indices) == 0)
    # Age difference between parents not bigger than 5 years
    married_male_mask = zeros(Int, length(age_vector))
    married_male_mask[married_male_indices] .= 1

    married_female_mask = zeros(Int, length(age_vector))
    married_female_mask[married_female_indices] .= 1
    
    for hh_id in hh_size2_indices
        active_married_male = sum(allocation[:, hh_id] .* married_male_mask)
        active_married_female = sum(allocation[:, hh_id] .* married_female_mask)
        
        total_age_male = sum(allocation[:, hh_id] .* married_male_mask .* age_vector)
        total_age_female = sum(allocation[:, hh_id] .* married_female_mask .* age_vector)

        # Soft constraint with penalty
        @constraint(model, active_married_female * (total_age_male - total_age_female) <= 5 + penalty[hh_id])
        @constraint(model, active_married_male * (total_age_female - total_age_male) <= 5 + penalty[hh_id])
    end

    # Constraints for households of size 3 or more
    # There must be at least 1 adult 
    @constraint(model, [hh_id=hh_size3plus_indices], sum(allocation[indv_id, hh_id] for indv_id in adult_indices) >= 1) 
    # There must be at least 1 parent 
    @constraint(model, [hh_id=hh_size3plus_indices], sum(allocation[indv_id, hh_id] for indv_id in parent_indices) >= 1) 
    # There can be at most 1 married adult male 
    @constraint(model, [hh_id=hh_size3plus_indices], sum(allocation[indv_id, hh_id] for indv_id in married_male_indices) <= 1) 
    # There can be at most 1 married adult female 
    @constraint(model, [hh_id=hh_size3plus_indices], sum(allocation[indv_id, hh_id] for indv_id in married_female_indices) <= 1) 
    # The number of children in the household cannot exceed the household's capacity minus 2 parents
    @constraint(model, [idx=1:length(hh_size3plus_indices)], sum(allocation[indv_id, hh_size3plus_indices[idx]] for indv_id in child_indices) <= hh_size3plus_capacity[idx] - 2)
    
    # Optimize the model to find the best allocation of individuals to households
    optimize!(model)
    println("Objective value: ", objective_value(model))

    # Retrieve the allocation results from the model
    allocation_values = value.(allocation)
    penalty = value.(penalty)

    return allocation_values, penalty  # Return the allocation matrix
end


"""
    disaggr_optimized_indiv(allocation_values, aggregated_individuals::DataFrame)

Disaggregate individuals into households based on allocation results.

# Arguments
- `allocation_values::Matrix{Float64}`: A matrix where each row corresponds to an individual and each column corresponds to a household, indicating whether the individual is allocated to the household.
- `aggregated_individuals::DataFrame`: A DataFrame containing the aggregated data of individuals, including identifiers and demographic details. 

# Returns
- `DataFrame`: A DataFrame containing disaggregated individuals.
"""
function disaggr_optimized_indiv(allocation_values::Matrix{Float64}, aggregated_individuals::DataFrame)
    # Initialize cumulative populations for disaggregation    
    cumulative_population_ind = cumsum(aggregated_individuals[!, POPULATION_COLUMN])
    individuals_count = sum(aggregated_individuals[!,POPULATION_COLUMN])
    # Disaggregate individuals based on allocation results
    disaggregated_individuals = DataFrame(
        id = 1:individuals_count,
        agg_ind_id = Vector{Union{Int,Missing}}(missing, individuals_count),
        household_id = Vector{Union{Int,Missing}}(missing, individuals_count),
    )
    for individual_id = 1:individuals_count

        # Assign individual ID to disaggregated_individuals
        agg_ind_id = findrow(cumulative_population_ind, individual_id)
        disaggregated_individuals[individual_id, :agg_ind_id] =
            aggregated_individuals[agg_ind_id, :id]

        # Assign household ID to disaggregated individuals
        household_id = findfirst(x -> x == 1.0, allocation_values[individual_id, :])
        if household_id === nothing
            disaggregated_individuals[individual_id, :household_id] = missing
        else
            disaggregated_individuals[individual_id, :household_id] = household_id
        end
    end
    return disaggregated_individuals
end  


"""
    disaggr_optimized_hh(allocation_values, aggregated_households, aggregated_individuals, parent_indices)

Disaggregates household data based on optimized allocation results, creating a new DataFrame of disaggregated households.

# Arguments
- `allocation_values::Matrix{Float64}`: A matrix indicating the optimized allocation of individuals to households.
- `aggregated_households::DataFrame`: A DataFrame containing aggregated household data.
- `aggregated_individuals::DataFrame`: A DataFrame containing individual data.
- `parent_indices::Vector{Int}`: A Vector of indices of individuals classified as parents.

# Returns
- `DataFrame`: A DataFrame representing disaggregated households.
"""
function disaggr_optimized_hh(allocation_values::Matrix{Float64}, aggregated_households::DataFrame, aggregated_individuals::DataFrame, parent_indices::Vector{Int})
    # Initialize cumulative populations for disaggregation 
    cumulative_population_hh = cumsum(aggregated_households[!, POPULATION_COLUMN])
    cumulative_population_ind = cumsum(aggregated_individuals[!, POPULATION_COLUMN])
    households_count = sum(aggregated_households[!,POPULATION_COLUMN])

    # Disaggregate households based on allocation results
    max_household_size = maximum(aggregated_households[:, HOUSEHOLD_SIZE_COLUMN])
    household_columns = [:agg_hh_id, :head_id, :partner_id]  # Initialize with parent columns
    for i = 1:(max_household_size-2)
        push!(household_columns, Symbol("child$(i)_id"))
    end
    disaggregated_households = DataFrame(id = 1:households_count)
    for column in household_columns
        disaggregated_households[!, column] =
            Vector{Union{Int,Missing}}(missing, households_count)
    end

    for household_id = 1:households_count

        # Add household ID from aggregated_households
        agg_hh_id = findfirst(x -> x >= household_id, cumulative_population_hh)
        disaggregated_households[household_id, :agg_hh_id] =
            aggregated_households[agg_hh_id, ID_COLUMN]

        # Assign parents and children
        assigned_individuals = findall(x -> x == 1.0, allocation_values[:, household_id])
        if length(assigned_individuals) == 1
            individual_id = findrow(cumulative_population_ind, assigned_individuals[1])
            disaggregated_households[household_id, :head_id] =
                aggregated_individuals[individual_id, :id]
        elseif length(assigned_individuals) >= 2
            parents = intersect(assigned_individuals, parent_indices)
            individual_id = findrow(cumulative_population_ind, parents[1])
            disaggregated_households[household_id, :head_id] =
                aggregated_individuals[individual_id, :id]
            if length(parents) == 2
                individual_id = findrow(cumulative_population_ind, parents[2])
                disaggregated_households[household_id, :partner_id] =
                    aggregated_individuals[individual_id, :id]
            end
            children = setdiff(assigned_individuals, parents)
            child_count = 0
            for child_id in children
                child_count += 1
                individual_id = findrow(cumulative_population_ind, child_id)
                disaggregated_households[household_id, Symbol("child$(child_count)_id")] =
                    aggregated_individuals[individual_id, :id]
            end
        end
    end
    return disaggregated_households
end