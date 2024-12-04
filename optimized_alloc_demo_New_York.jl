using Pkg
Pkg.activate(".")
Pkg.instantiate()
using Revise
using DataFrames
using SyntheticPopulation



#each individual and each household represent 100.000 individuals or households
SCALE = 0.0001/3 

#all values are based on China census data
individual_popoulation_size = 8258035


#individuals
population = [
    # male
    227349
    ,223900
    ,244087
    ,228152
    ,237353
    ,315374
    ,347733
    ,295564
    ,270094
    ,237024
    ,244597
    ,236294
    ,246669
    ,201415
    ,162392
    ,112733
    ,68463
    ,61740
    # female
    ,218215
    ,213700
    ,233291
    ,222591
    ,253345
    ,347152
    ,353146
    ,304273
    ,275160
    ,256192
    ,264607
    ,268286
    ,264165
    ,237136
    ,205842
    ,160794
    ,111421
    ,107786
]


marginal_ind_age_sex = DataFrame(
    sex = repeat(['M', 'F'], inner=18),
    age = repeat(2:5:87, outer=2), 
    population = SCALE * population
    )
    
marginal_ind_sex_maritalstatus = DataFrame(
    sex = repeat(['M', 'F'], 4), 
    maritalstatus = repeat(["Never_married", "Married", "Divorced", "Widowed"], inner = 2), 
    population = SCALE .* [1469519, 1536292, 1515237, 1470917, 205732, 341398, 75108, 286919]
    )


marginal_ind_income = DataFrame(
    income = [9999, 14999, 24999, 34999, 49999, 64999, 74999, 99999, 100000], 
    population = Int.(round.(SCALE * individual_popoulation_size .* [0.014, 0.014, 0.05, 0.093, 0.16, 0.139, 0.074, 0.144, 0.311]))
    )


#households
household_total_population = 3394750
marginal_hh_size = DataFrame(
    hh_size = [1,2,3,4],
    population = Int.(round.(SCALE * household_total_population .* [0.345, 0.297, 0.15, 0.209]))
    )

#generation of dataframe of individuals
aggregated_individuals = generate_joint_distribution(marginal_ind_sex_maritalstatus, marginal_ind_income, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file_NY.json")
filter!(row -> row[SyntheticPopulation.POPULATION_COLUMN] >= 1, aggregated_individuals)
aggregated_individuals.id = 1:nrow(aggregated_individuals)
aggregated_individuals = add_indices_range_to_indiv(aggregated_individuals)
aggregated_individuals = add_individual_flags(aggregated_individuals)
adult_indices, married_male_indices, married_female_indices, parent_indices, child_indices, age_vector = prep_group_indices_for_indv_constraints(aggregated_individuals);



#generation of dataframe of households
aggregated_households = generate_joint_distribution(marginal_hh_size)
aggregated_households = add_indices_range_to_hh(aggregated_households)
hh_size1_indices, hh_size2_indices, hh_size3plus_indices, hh_capacity = prep_group_indices_for_hh_constraints(aggregated_households);

# Optimization
allocation_values, household_inhabited, household_married_male, household_married_female, penalty, female_parent_relaxation, male_parent_relaxation = define_and_run_optimization(aggregated_individuals
    , aggregated_households
    
    , hh_size1_indices
    , hh_size2_indices
    , hh_size3plus_indices
    , hh_capacity

    , adult_indices
    , married_male_indices
    , married_female_indices
    , parent_indices
    , child_indices
    , age_vector);  

disaggregated_individuals = disaggr_optimized_indiv(allocation_values, aggregated_individuals)
disaggregated_households = disaggr_optimized_hh(allocation_values, aggregated_households, aggregated_individuals, parent_indices)


# Define a function to perform the join for each role (head, partner, child1, etc.)
function join_individual_data(households_df, individuals_df, role_id::Symbol, suffix::String)

    # Perform the left join
    join_df = leftjoin(
        households_df,
        individuals_df,
        on=role_id => :id,
        makeunique=true,
        matchmissing = :equal
    )
    
    # Rename the joined columns with suffixes for clarity
    rename!(join_df, Dict(Symbol("maritalstatus") => Symbol("maritalstatus_$suffix"), 
                     Symbol("income") => Symbol("income_$suffix"),
                     Symbol("sex") => Symbol("sex_$suffix"),
                     Symbol("age") => Symbol("age_$suffix"),
                     Symbol("is_potential_parent") => Symbol("is_potential_parent_$suffix"),
                     Symbol("is_potential_child") => Symbol("is_potential_child_$suffix")))
    foreach(col -> join_df[!, col] = coalesce.(join_df[!, col], ""), names(join_df))
    join_df[!,Symbol("attributes_$suffix")] = string.(join_df[!,Symbol("sex_$suffix")], "|", join_df[!,Symbol("age_$suffix")], "|", join_df[!,Symbol("maritalstatus_$suffix")])
    return join_df
end

# Sequentially join for each role and add relevant data
disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :head_id, "head")
disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :partner_id, "partner")
disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :child1_id, "child1")
disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :child2_id, "child2")
disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :child3_id, "child3")
#disaggregated_households = join_individual_data(disaggregated_households, aggregated_individuals, :child4_id, "child4")

disaggregated_households = leftjoin(
        disaggregated_households,
        aggregated_households,
        on=:agg_hh_id => :id,
        makeunique=true,
        matchmissing = :equal
    )

report_disaggregated_households = disaggregated_households[!, ["id", "hh_size", "attributes_head", "attributes_partner", "attributes_child1", "attributes_child2", "attributes_child3"]]#, "attributes_child4"
report_disaggregated_households[rand(1:nrow(report_disaggregated_households), 10),:]


household_married_female[47]
household_married_male[47]
penalty[47]




function join_and_rename!(df1::DataFrame, df2::DataFrame, column_name::Symbol)
    df_joined = leftjoin(df1, df2, on = column_name => :id, makeunique=true, matchmissing = :notequal)

    # Rename the new columns
    for col in names(df2)[2:end]  # Skip the id column
        rename!(df_joined, Symbol(col) => Symbol(replace(string(column_name), "_id" => "_"*col)))
    end

    return df_joined
end

# Apply the function to each id column in df1
id_columns = [:head_id, :partner_id, :child1_id, :child2_id, :child3_id]
disaggregated_households_joined = disaggregated_households
for column_name in id_columns
    disaggregated_households_joined = join_and_rename!(disaggregated_households_joined, aggregated_individuals, column_name)
end



print("check that there is a proper age difference between children and parents (between 20 and 40)\n")
for parent in [:head_age, :partner_age]
    println("PARENT")
    for child in [:child1_age, :child2_age, :child3_age]
        print(unique(collect(skipmissing(disaggregated_households_joined[!, parent] - disaggregated_households_joined[!, child]))))
        print("\n")
    end
end
print("\n\n")

print("check that for all assigned individuals, the value in column :population from aggregated_individuals is larger than 1\n")
for column in [:head_population, :partner_population, :child1_population, :child2_population, :child3_population]
    print(unique(collect(skipmissing(disaggregated_households_joined[!, column]))))
end


# check that there is a proper age difference between children and parents (between 20 and 40)
# PARENT
# [35, 40, 30, 25, 20, 15, 10, 0, -5, -10, -15, -20, -25, 5]
# [35, 40, 30, 25, 20, 15, 10, 0, -5, -10, -15, -20, -25]
# [35, 40, 30, 25, 20, 15, 10]
# PARENT
# [30, 35, 40]
# [30]
# Union{}[]


# check that for all assigned individuals, the value in column :population from aggregated_individuals is larger than 1
# [11, 9, 5, 12, 14, 18, 16, 3, 2][2, 12, 10, 8, 5, 3, 14, 19, 16][4, 5, 1, 3, 2, 10, 9][4, 1, 5, 3, 2, 10][4, 1, 5, 3]