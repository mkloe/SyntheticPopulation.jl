using JuMP
using GLPK
using DataFrames
using ProgressMeter


function findrow(cumulative_population, individual_id)
    for i = 1:length(cumulative_population)
        if individual_id <= cumulative_population[i]
            return i
        end
    end
    return 0  # If individual_id is below the last index, return the last index
end


function add_household_size_constraints!(
    model,
    allocation,
    household_index,
    household_capacity,
    married_male_indices,
    married_female_indices,
    adult_indices,
    parent_indices,
    child_indices,
    age_difference_pairs,
    parent_child_pairs,
)
    # Total number of individuals in a household cannot be larger than household_capacity
    @constraint(model, sum(allocation[:, household_index]) <= household_capacity)

    if household_capacity == 1
        @constraint(model, sum(allocation[adult_indices, household_index]) <= 1) # There is 0 or 1 adult in a household
        @constraint(model, sum(allocation[child_indices, household_index]) == 0) # There are 0 children in a household

    elseif household_capacity == 2
        @constraint(model, sum(allocation[adult_indices, household_index]) >= 1) # There is 1 or more adult in a household
        #= TODO:commented out because of computing complexity
        @constraint(
            model,
            [(male_id, female_id) in age_difference_pairs],
            allocation[male_id, household_index] + allocation[female_id, household_index] <= 1
            ) # There is maximum 1 person from each pair of male-female adults that have too large age gap =#
        @constraint(model, sum(allocation[married_male_indices, household_index]) == 1) # There is exactly one married adult male; TODO: could be <= 1
        @constraint(model, sum(allocation[married_female_indices, household_index]) == 1) # There is exactly one married adult female; TODO: could be <= 1
        @constraint(model, sum(allocation[child_indices, household_index]) == 0) # There are 0 children in a household; TODO: could be <= household_capacity - number of parents)

    elseif household_capacity >= 3
        @constraint(model, sum(allocation[adult_indices, household_index]) >= 1) # There is 1 or more adult in a household
        @constraint(model, sum(allocation[parent_indices, household_index]) >= 1) # There is 0 or 1 parent in a household
        @constraint(model, sum(allocation[married_male_indices, household_index]) <= 1) # There is 0 or 1 married adult male
        @constraint(model, sum(allocation[married_female_indices, household_index]) <= 1)# There is 0 or 1 married adult female
        #= TODO: commented out because of computing complexity
        @constraint(
            model,
            [(male_id, female_id) in age_difference_pairs],
            allocation[male_id, household_index] + allocation[female_id, household_index] <= 1
            ) # There is maximum 1 person from each pair of male-female adults that have too large age gap =#
        @constraint(
            model,
            sum(allocation[child_indices, household_index]) <= household_capacity - 2
            ) # TODO: could be <= household capacity - sum of assigned parents
        #= TODO: commented out because of computing complexity
        @constraint(
            model,
            [(parent_id, child_id) in parent_child_pairs],
            allocation[parent_id, household_index] + allocation[child_id, household_index] <= 1
            ) # There is maximum 1 person from each pair of parent-child that have too large age gap -=#
    end
end


function add_household_constraints!(
    model,
    allocation,
    aggregated_individuals,
    aggregated_households,
)
    println("Preparation for creation of household constraints started.")
    household_index = 1
    married_male_indices = []
    married_female_indices = []
    parent_indices = []
    adult_indices = []
    child_indices = []
    parent_individual_index = 1
    adult_individual_index = 1
    cumulative_population = cumsum(aggregated_individuals[!, POPULATION_COLUMN])

    # Collect indices of the adults
    progress_household_constraint_preparation_1 =
        Progress(nrow(aggregated_individuals), 1, "Preparing household constraints 1/3")
    progress_household_constraint_preparation_1.printed = true
    for row in eachrow(aggregated_individuals)
        if row[AGE_COLUMN] >= MINIMUM_ADULT_AGE
            for _ = 1:row[POPULATION_COLUMN]
                push!(adult_indices, adult_individual_index)
                adult_individual_index += 1
            end
        else
            for _ = 1:row[POPULATION_COLUMN]
                adult_individual_index += 1
            end
        end

        # Collect indices of the married adult females and married adult male
        if row[AGE_COLUMN] >= MINIMUM_ADULT_AGE &&
           row[MARITALSTATUS_COLUMN] == AVAILABLE_FOR_MARRIAGE
            for _ = 1:row[POPULATION_COLUMN]
                if row[SEX_COLUMN] == 'M'
                    push!(married_male_indices, parent_individual_index)
                elseif row[SEX_COLUMN] == 'F'
                    push!(married_female_indices, parent_individual_index)
                end
                push!(parent_indices, parent_individual_index)
                parent_individual_index += 1
            end
        else
            for _ = 1:row[POPULATION_COLUMN]
                push!(child_indices, parent_individual_index)
                parent_individual_index += 1
            end
        end
        ProgressMeter.next!(progress_household_constraint_preparation_1)
    end
    finish!(progress_household_constraint_preparation_1)

    # Collect pairs of indices of parents that do not meet age difference criteria 
    progress_household_constraint_preparation_2 =
        Progress(length(married_male_indices), 1, "Preparing household constraints 2/3")
    progress_household_constraint_preparation_2.printed = true
    age_difference_pairs = []
    for male_index in married_male_indices
        male_age =
            aggregated_individuals[findrow(cumulative_population, male_index), AGE_COLUMN]
        for female_index in married_female_indices
            female_age = aggregated_individuals[
                findrow(cumulative_population, female_index),
                AGE_COLUMN,
            ]
            if abs(male_age - female_age) > 5
                push!(age_difference_pairs, (male_index, female_index))
            end
        end
        ProgressMeter.next!(progress_household_constraint_preparation_2)
    end
    finish!(progress_household_constraint_preparation_2)

    # Collect pairs of indices of parents and children that do not meet age difference criteria 
    progress_household_constraint_preparation_3 =
        Progress(length(parent_indices), 1, "Preparing household constraints 3/3")
    progress_household_constraint_preparation_3.printed = true
    parent_child_pairs = []
    index_parent = 1
    index_child = 1
    for parent_id in parent_indices
        index_parent = findrow(cumulative_population, parent_id)
        for child_id in child_indices
            index_child = findrow(cumulative_population, child_id)
            age_difference =
                aggregated_individuals[index_parent, AGE_COLUMN] -
                aggregated_individuals[index_child, AGE_COLUMN]
            if age_difference < MINIMUM_ADULT_AGE || age_difference > 40
                push!(parent_child_pairs, (parent_id, child_id))
            end
        end
        ProgressMeter.next!(progress_household_constraint_preparation_3)
    end
    finish!(progress_household_constraint_preparation_3)
    println("Preparation for creation of household constraints finished.")

    # Add constraints based on precomputed indices for each of the household
    println("Creation of household constraints started.")
    progress_household_constraint_assignment =
        Progress(nrow(aggregated_households), 1, "Adding household constraints.")
    progress_household_constraint_assignment.printed = true
    for row in eachrow(aggregated_households)
        household_capacity = row[HOUSEHOLD_SIZE_COLUMN]
        population_size = row[POPULATION_COLUMN]
        for _ = 1:population_size
            add_household_size_constraints!(
                model,
                allocation,
                household_index,
                household_capacity,
                married_male_indices,
                married_female_indices,
                adult_indices,
                parent_indices,
                child_indices,
                age_difference_pairs,
                parent_child_pairs,
            )

            household_index += 1
        end
        ProgressMeter.next!(progress_household_constraint_assignment)
    end
    finish!(progress_household_constraint_assignment)
    println("Creation of household constraints finished")

    return adult_indices, parent_indices, child_indices
end


function add_individual_assignment_constraints!(model, allocation, aggregated_individuals)
    # Initialize variables and progress bar
    individual_index = 1
    progress_individual_constraints =
        Progress(nrow(aggregated_individuals), 1, "Adding individual constraints")
    progress_individual_constraints.printed = true

    # Add a constraint: each individual can only be assigned once
    println("Creation of individual constraints started.")
    for row in eachrow(aggregated_individuals)
        population_size = row[POPULATION_COLUMN]
        for _ = 1:population_size
            @constraint(model, sum(allocation[individual_index, :]) <= 1)
            individual_index += 1
        end
        ProgressMeter.next!(progress_individual_constraints)
    end
    finish!(progress_individual_constraints)
    println("Creation of individual constraints finished.")
end


function assign_and_optimize_individuals_to_households!(
    aggregated_individuals::DataFrame,
    aggregated_households::DataFrame,
)
    # Prepare dataframes for processing
    aggregated_individuals_df =
        aggregated_individuals[aggregated_individuals[:, POPULATION_COLUMN].>0, :]
    aggregated_households_df =
        aggregated_households[aggregated_households[:, POPULATION_COLUMN].>0, :]
    individuals_count = sum(aggregated_individuals_df[:, POPULATION_COLUMN])  # Total number of individuals
    households_count = sum(aggregated_households_df[:, POPULATION_COLUMN])  # Total number of households

    # Show initial statistics
    println("Total number of individuals: ", individuals_count)
    println("Total number of households: ", households_count)
    println("Allocation started...")

    # Define model
    model = Model(GLPK.Optimizer)
    @variable(model, allocation[1:individuals_count, 1:households_count], Bin)  # Define decision variables
    @objective(model, Max, sum(allocation))  # Define objective function: maximize the number of assigned individuals
    add_individual_assignment_constraints!(model, allocation, aggregated_individuals_df)
    adult_indices, parent_indices, child_indices = add_household_constraints!(
        model,
        allocation,
        aggregated_individuals_df,
        aggregated_households_df,
    )

    # Optimization
    println("Optimization of allocation started.")
    optimize!(model)

    # Show final statistics
    println("Optimization completed.")
    println("Objective value: ", objective_value(model))

    # Retrieve the allocation results
    allocation_values = value.(allocation)

    # Initialize cumulative populations for disaggregation    
    cumulative_population_hh = cumsum(aggregated_households_df[!, POPULATION_COLUMN])
    cumulative_population_ind = cumsum(aggregated_individuals_df[!, POPULATION_COLUMN])

    # Disaggregate individuals based on allocation results
    progress_individuals = Progress(individuals_count, 1, "Disaggregating individuals")
    progress_individuals.printed = true
    disaggregated_individuals = DataFrame(
        id = 1:individuals_count,
        agg_ind_id = Vector{Union{Int,Missing}}(missing, individuals_count),
        household_id = Vector{Union{Int,Missing}}(missing, individuals_count),
    )
    for individual_id = 1:individuals_count

        # Assign individual ID to disaggregated_individuals
        agg_ind_id = findrow(cumulative_population_ind, individual_id)
        disaggregated_individuals[individual_id, :agg_ind_id] =
            aggregated_individuals_df[agg_ind_id, :id]

        # Assign household ID to disaggregated individuals
        household_id = findfirst(x -> x == 1.0, allocation_values[individual_id, :])
        if household_id === nothing
            disaggregated_individuals[individual_id, :household_id] = missing
        else
            disaggregated_individuals[individual_id, :household_id] = household_id
        end
        ProgressMeter.next!(progress_individuals)
    end
    finish!(progress_individuals)

    # Disaggregate households based on allocation results
    progress_households = Progress(households_count, 1, "Disaggregating households")
    progress_households.printed = true
    max_household_size = maximum(aggregated_households_df[:, HOUSEHOLD_SIZE_COLUMN])
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
            aggregated_households_df[agg_hh_id, ID_COLUMN]

        # Assign parents and children
        assigned_individuals = findall(x -> x == 1.0, allocation_values[:, household_id])
        if length(assigned_individuals) == 1
            individual_id = findrow(cumulative_population_ind, assigned_individuals[1])
            disaggregated_households[household_id, :head_id] =
                aggregated_individuals_df[individual_id, :id]
        elseif length(assigned_individuals) >= 2
            parents = intersect(assigned_individuals, parent_indices)
            individual_id = findrow(cumulative_population_ind, parents[1])
            disaggregated_households[household_id, :head_id] =
                aggregated_individuals_df[individual_id, :id]
            if length(parents) == 2
                individual_id = findrow(cumulative_population_ind, parents[2])
                disaggregated_households[household_id, :partner_id] =
                    aggregated_individuals_df[individual_id, :id]
            end
            children = setdiff(assigned_individuals, parents)
            child_count = 0
            for child_id in children
                child_count += 1
                individual_id = findrow(cumulative_population_ind, child_id)
                disaggregated_households[household_id, Symbol("child$(child_count)_id")] =
                    aggregated_individuals_df[individual_id, :id]
            end
        end

        ProgressMeter.next!(progress_households)
    end
    finish!(progress_households)
    print("Allocation finished.")

    return model, allocation_values, disaggregated_individuals, disaggregated_households
end
