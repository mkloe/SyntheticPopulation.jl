using Pkg
Pkg.activate(".")
Pkg.instantiate()
using Revise
using DataFrames
using SyntheticPopulation




#each individual and each household represent 100.000 individuals or households
SCALE = 0.0001 

#all values are based on China census data
individual_popoulation_size = 21890000

#individuals
marginal_ind_age_sex = DataFrame(
    sex = repeat(['M', 'F'], 18),
    age = repeat(2:5:87, inner = 2), 
    population = SCALE .* 10000 .* [52.6, 49.0, 48.5, 44.8, 33.6, 30.6, 34.6, 28.8, 71.6, 63.4, 99.6, 90.9, 130.9, 119.4, 110.8, 103.5, 83.8, 76.4, 84.2, 77.7, 84.2, 77.8, 82.8, 79.9, 67.7, 71.0, 56.9, 62.6, 31.5, 35.3, 18.5, 23.0, 15.2, 19.7, 12.5, 16.0]
    )

marginal_ind_sex_maritalstatus = DataFrame(
    sex = repeat(['M', 'F'], 4), 
    maritalstatus = repeat(["Never_married", "Married", "Divorced", "Widowed"], inner = 2), 
    population = SCALE .* [1679, 1611, 5859, 5774, 140, 206, 128, 426] ./ 0.00082
    )

marginal_ind_income = DataFrame(
    income = [25394, 44855, 63969, 88026, 145915], 
    population = repeat([individual_popoulation_size * SCALE / 5], 5)
    )

#households
household_total_population = 8230000
marginal_hh_size = DataFrame(
    hh_size = [1,2,3,4,5],
    population = Int.(round.(SCALE * household_total_population .* [0.299, 0.331, 0.217, 0.09, 0.063]))
    )

#generation of dataframe of individuals
aggregated_individuals = generate_joint_distribution(marginal_ind_sex_maritalstatus, marginal_ind_income, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file.json")

#generation of dataframe of households
aggregated_households = generate_joint_distribution(marginal_hh_size)

model, allocation_values, disaggregated_individuals, disaggregated_households,timer = assign_and_optimize_individuals_to_households!(aggregated_individuals, aggregated_households)
println(timer)


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