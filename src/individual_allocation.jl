#=
Allocation of individuals into households is partially inspired by the approach from:
Ponge, J., Enbergs, M., Schüngel, M., Hellingrath, B., Karch, A., & Ludwig, S. (2021, December). 
Generating synthetic populations based on german census data. In 2021 Winter Simulation Conference (WSC) (pp. 1-12). IEEE.
=#


"""
    show_statistics(aggregated_individuals::DataFrame, aggregated_households::DataFrame, total_individual_population::Int, total_household_population::Int)

Auxilary function - it computes and prints statistics that summarize the process of allocation of individuals to households

Arguments:
- `aggregated_individuals` - the mutated data frame of individuals that aggregates all individuals that were not allocated
- `aggregated_households` - the mutated data frame of households that aggregates all households that were not allocated
- `total_individual_population` - total population of individuals at the beginning of allocation process
- `total_household_population` - total population of housheolds at the beginning of allocation process
"""
function show_statistics(aggregated_individuals::DataFrame, aggregated_households::DataFrame, total_individual_population::Int, total_household_population::Int)
    print("Allocated " * string(round(((total_individual_population - sum(aggregated_individuals[:,POPULATION_COLUMN])) / total_individual_population) * 100)) * "% individuals.\n")
    print("Allocated " * string(round(((total_household_population - sum(aggregated_households[:,POPULATION_COLUMN])) / total_household_population) * 100)) * "% households.\n")
end


"""
    update_dfs!(aggregated_idividuals::DataFrame, disaggregated_households::DataFrame, available_individuals::DataFrame, individual_index::Int, household_id::Int, individual_type::String)

Auxilary function - it updates data frames of individuals and households after allocation step

Arguments:
- `aggregated_idividuals` - data frame of aggregated individuals
- `disaggregated_households` - data frame that represents the generated population of disaggregated households
- `available_individuals` - data frame from which ID of selected individual is to be extracted
- `individual_index` - index of row o the selected individual in data frame `available_individuals`
- `household_id` - ID of the household, for which the allocation of individual was made
- `individual_type` - type of the individual that was allocated that indicates for which column the allocation is performed
"""
function update_dfs!(aggregated_idividuals::DataFrame, disaggregated_households::DataFrame, available_individuals::DataFrame, individual_index::Int, household_id::Int, individual_type::String)
    
    #deduct the individual from available individual pool
    individual_id = available_individuals[individual_index, ID_COLUMN]
    aggregated_idividuals[individual_id, POPULATION_COLUMN] -= 1 #set to 0 if we want drawing with replacement
    
    #assign individual ID to household
    disaggregated_households[household_id, Symbol(individual_type * "_id")] = individual_id
end


"""
    select_household_head!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_size::Int, household_id::Int) 

Auxilary function - it draws a household head that is allocated to household from all individuals that meet the criteria for allocation

Arguments:
- `aggregated_individuals` - data frame of aggregated individuals
- `disaggregated_households` - data frame that represents the generated population of disaggregated households
- `hh_size` - number of members of the houehold for which the allocation is done
- `household_id` - ID of the household for which the allocation is done
"""
function select_household_head!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_size::Int, household_id::Int) 
    
    #filter criteria
    available_heads = copy(aggregated_individuals)
    available_heads = filter(POPULATION_COLUMN => >(0), available_heads)
    available_heads = filter(AGE_COLUMN => >=(MINIMUM_ADULT_AGE), available_heads)
    
    if hh_size > 1
        available_heads = filter(MARITALSTATUS_COLUMN => ==(AVAILABLE_FOR_MARRIAGE), available_heads)
    else
        available_heads = filter(MARITALSTATUS_COLUMN => !=(AVAILABLE_FOR_MARRIAGE), available_heads)
    end
    
    if hh_size > 2
        available_heads = filter(AGE_COLUMN => <=(40 + MINIMUM_ADULT_AGE), available_heads)
    end
    
    if nrow(available_heads) == 0
        print("\n---------------\nThere are no available heads! \n---------------\n")
        return Dict("status" => 1)
    end

    #update disaggregated_households with the selected id
    individual_index = sample(collect(1:nrow(available_heads)), Weights(available_heads[:,POPULATION_COLUMN]))
    update_dfs!(aggregated_individuals, disaggregated_households, available_heads, individual_index, household_id, "head")
    
    #extract needed information for the household head
    hh_head_sex = available_heads[individual_index, SEX_COLUMN]
    hh_head_age = available_heads[individual_index, AGE_COLUMN]

    return Dict("status" => 0, "result" => (hh_head_sex, hh_head_age))
end


"""
    select_partner!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_head_sex::Union{String, Char}, hh_head_age::Int, household_id::Int)

Auxilary function - it draws a household partner that is allocated to household from all individuals that meet the criteria for allocation

Arguments:
- `aggregated_individuals` - data frame of aggregated individuals
- `disaggregated_households` - data frame that represents the generated population of disaggregated households
- `hh_head_sex` - sex of the household head, for which the partner is allocated
- `hh_head_age` - age of the household head, for which the partner is allocated
- `household_id` - ID of the household for which the allocation is done
"""
function select_partner!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, hh_head_sex::Union{String, Char}, hh_head_age::Int, household_id::Int)
    
    #filter criteria
    available_partners = copy(aggregated_individuals)
    available_partners = filter(POPULATION_COLUMN => >(0), available_partners)
    available_partners = filter(AGE_COLUMN => >(MINIMUM_ADULT_AGE), available_partners)
    #available_partners = filter(AGE_COLUMN => <=(hh_head_age + 5), available_partners)
    #available_partners = filter(AGE_COLUMN => >=(hh_head_age - 5), available_partners)
    available_partners = filter(SEX_COLUMN => !=(hh_head_sex), available_partners)
    available_partners = filter(MARITALSTATUS_COLUMN => ==(AVAILABLE_FOR_MARRIAGE), available_partners)

    if nrow(available_partners) == 0
        print("\n---------------\nThere are no available partners! \n---------------\n")
        return Dict("status" => 1)
    end

    #update disaggregated_households with the selected id
    individual_index = sample(collect(1:nrow(available_partners)), Weights(available_partners[:,POPULATION_COLUMN]))
    update_dfs!(aggregated_individuals, disaggregated_households, available_partners, individual_index, household_id, "partner")

    #extract needed information for the household head
    partner_age = available_partners[individual_index, AGE_COLUMN]

    return Dict("status" => 0, "result" => partner_age)
end


"""
    select_child!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, child_number::Int, hh_head_age::Int, partner_age::Int, household_id::Int)

Auxilary function - it draws a household child that is allocated to household from all individuals that meet the criteria for allocation

Arguments:
- `aggregated_individuals` - data frame of aggregated individuals
- `disaggregated_households` - data frame that represents the generated population of disaggregated households
- `child_number` - number of the child among all children in the household, for which the allocation is done 
- `hh_head_age` - age of the household head, for which the child is allocated
- `partner_age` - age of the household partner, for which the child is allocated
- `household_id` - ID of the household for which the allocation is done
"""
function select_child!(aggregated_individuals::DataFrame, disaggregated_households::DataFrame, child_number::Int, hh_head_age::Int, partner_age::Int, household_id::Int)
   
    #filter criteria
    available_children = copy(aggregated_individuals)
    available_children = filter(POPULATION_COLUMN => >(0), available_children)
    available_children = filter(AGE_COLUMN => <(MINIMUM_ADULT_AGE), available_children)
    #available_children = filter(AGE_COLUMN => >=(max(hh_head_age, partner_age) - 40), available_children)
    #available_children = filter(AGE_COLUMN => <=(min(hh_head_age, partner_age) - 15), available_children)

    if nrow(available_children) == 0
        print("\n---------------\nThere are no available children! \n---------------\n")
        return Dict("status" => 1)
    end

    child_column = "child" * string(child_number)
    individual_index = sample(collect(1:nrow(available_children)), Weights(available_children[:,POPULATION_COLUMN]))
    update_dfs!(aggregated_individuals, disaggregated_households, available_children, individual_index, household_id, child_column)

    return Dict("status" => 0)

end


"""
    allocate_household_members!(disaggregated_households::DataFrame, aggregated_individuals::DataFrame, hh_size::Int)

Auxilary function - it mutates a dataframe disaggregated_households by allocating individuals to a randomly selected housheold

Arguments:
- `aggregated_individuals` - data frame of aggregated individuals
- `disaggregated_households` - data frame that represents the generated population of disaggregated households
- `hh_size` - size of the household, for which the household is done
"""
function allocate_household_members!(disaggregated_households::DataFrame, aggregated_individuals::DataFrame, hh_size::Int)
    
    #select household_id for which the members will be allocated
    available_households = copy(disaggregated_households)
    available_households = filter(:individuals_allocated => ==(false), available_households)
    available_households = filter(Symbol(HOUSEHOLD_SIZE_COLUMN) => ==(hh_size), available_households)
    if(nrow(available_households)) == 0
        return 1
    else
        row_index = sample(collect(1:nrow(available_households)))
        household_id = available_households[row_index, ID_COLUMN]
    end

    #allocating households
    response = select_household_head!(aggregated_individuals, disaggregated_households, hh_size, household_id)
    if response["status"] == 1
        return 1
    else
        hh_head_sex, hh_head_age = response["result"]
    end
    
    #allocating partner
    if hh_size > 1
        response = select_partner!(aggregated_individuals, disaggregated_households, hh_head_sex, hh_head_age, household_id)
        if response["status"] == 1
            return 1
        else
            partner_age = response["result"]
        end
    end

    #allocating children
    if hh_size > 2
        for i in 3:hh_size
            child_number = i - 2
            response = select_child!(aggregated_individuals, disaggregated_households, child_number, hh_head_age, partner_age, household_id)
            if response["status"] == 1
                return 1
            end
        end
    end
    
    #set the household with this ID to unavailable
    disaggregated_households[household_id, :individuals_allocated] = true
    return 0
end


"""
    spread(vals,cnts)

Auxilary function - used in `expand_df_from_row_counts`
"""
function spread(vals,cnts) 
    [v for (v,c) in zip(vals, cnts) for i in 1:c]
end


"""
    expand_df_from_row_counts(dataframe::DataFrame)

Auxilary function - it returns a data frame that repeats each row from column `POPULATION_COLUMN` n times.

Arguments:
- `dataframe` - data frame that contains at least column `POPULATION_COLUMN` with integer values.
"""
function expand_df_from_row_counts(dataframe::DataFrame)
    
    df = copy(dataframe)
    rename!(df, ID_COLUMN => :hh_attr_id) #can be parametrized to avoid hardcoding

    result = combine(df, All() .=> (x -> spread(x, df[:,POPULATION_COLUMN])) .=> All())
    select!(result, Not([POPULATION_COLUMN]))
    id = collect(1:nrow(result))
    insertcols!(result, 1, ID_COLUMN => id)
    result.:individuals_allocated = repeat([false], nrow(result))
    
    return result
end


"""
    assign_individuals_to_households(aggregated_individuals::DataFrame, aggregated_households::DataFrame; return_unassigned::Bool = false)

Main function - it returns a disaggregated population of households, each of them characterized by a set of attributes and having individuals assigned to them.

Arguments:
- `aggregated_individuals` - a data frame of aggregated population of individuals. More information specified in `notebooks/dataframe_formats.ipynb`
- `aggregated_households` - a data frame of aggregated population of households. More information specified in `notebooks/dataframe_formats.ipynb`
- `return_unassigned` - an optional argument indicating whether the data frames with individuals and households that were not assigned should be returned.
"""
function assign_individuals_to_households(aggregated_individuals::DataFrame, aggregated_households::DataFrame; return_unassigned::Bool = false)
    
    #prepare dataframes for processing
    aggregated_individuals_df = copy(aggregated_individuals)
    aggregated_households_df = copy(aggregated_households)

    disaggregated_households = expand_df_from_row_counts(aggregated_households_df)
    disaggregated_households.:head_id = Int.(zeros(nrow(disaggregated_households)))
    disaggregated_households.:partner_id = Int.(zeros(nrow(disaggregated_households)))
    if maximum(disaggregated_households[:, HOUSEHOLD_SIZE_COLUMN]) > 2
        for i in collect(3:maximum(disaggregated_households[:, HOUSEHOLD_SIZE_COLUMN]))
            column_name = "child"*string(i-2)*"_id"
            disaggregated_households[:, column_name] = Int.(zeros(nrow(disaggregated_households)))
        end
    end
    
    #initiate values for calculating statistics
    total_household_population = sum(aggregated_households_df[:,POPULATION_COLUMN])
    total_individual_population = sum(aggregated_individuals_df[:,POPULATION_COLUMN])

    #show statistics
    print("Total number of individuals: ", total_individual_population, "\n")
    print("Total number of households: ", total_household_population, "\n")
    print("Allocation started... \n")

    @showprogress for _ in 1:sum(aggregated_households_df[:,POPULATION_COLUMN])

        #select households which are still available
        aggregated_households_df = filter(POPULATION_COLUMN => >(0), aggregated_households_df)
        nrow(aggregated_households_df) == 0 ? break : nothing

        #allocate household members
        row_index = sample(collect(1:nrow(aggregated_households_df)), Weights(aggregated_households_df[:,POPULATION_COLUMN]))
        hh_size = aggregated_households_df[row_index, HOUSEHOLD_SIZE_COLUMN]
        response = allocate_household_members!(disaggregated_households, aggregated_individuals_df, hh_size)
        aggregated_households_df[row_index, POPULATION_COLUMN] -= 1

        #no more available households or individuals
        response == 1 ? break : nothing
    end

    #show statistics and prepare results that are to be returned
    show_statistics(aggregated_individuals_df, aggregated_households_df, total_individual_population, total_household_population)
    aggregated_individuals_df = filter(POPULATION_COLUMN => >(0), aggregated_individuals_df)
    aggregated_households_df = filter(POPULATION_COLUMN => >(0), aggregated_households_df)

    #delete unneeded columns
    hh_colnames = names(aggregated_households)
    deleteat!(hh_colnames, findall(x -> (x in [string(ID_COLUMN), string(POPULATION_COLUMN)]), hh_colnames))
    disaggregated_households = disaggregated_households[:, Not(hh_colnames)]

    if return_unassigned == true
        unassigned = Dict{String, DataFrame}()
        unassigned["unassigned_households"] = aggregated_households_df
        unassigned["unassigned_individuals"] = aggregated_individuals_df
        return disaggregated_households, unassigned
    else
        return disaggregated_households
    end
end


#=
Ideas for further development:
- do not stop the function on first error (e.g. there can be still adults for one-person households if children are not used)
- parametrize filtering functions
=#