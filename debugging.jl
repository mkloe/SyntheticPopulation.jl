using DataFrames
using SyntheticPopulation


#each individual and each household represent 1000 individuals or households
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

#=
#checking if merging dfs in different order gives the same results
aggregated_individuals = generate_joint_distribution(marginal_ind_sex_maritalstatus, marginal_ind_age_sex, marginal_ind_income, config_file = "tutorial_notebooks/config_file.json")
print(sum(aggregated_individuals.:population), "\n")
aggregated_individuals = generate_joint_distribution(marginal_ind_age_sex, marginal_ind_sex_maritalstatus, marginal_ind_income, config_file = "tutorial_notebooks/config_file.json")
print(sum(aggregated_individuals.:population), "\n")
aggregated_individuals = generate_joint_distribution(marginal_ind_age_sex, marginal_ind_income, marginal_ind_sex_maritalstatus, config_file = "tutorial_notebooks/config_file.json")
print(sum(aggregated_individuals.:population), "\n")
aggregated_individuals = generate_joint_distribution(marginal_ind_income, marginal_ind_age_sex, marginal_ind_sex_maritalstatus, config_file = "tutorial_notebooks/config_file.json")
print(sum(aggregated_individuals.:population), "\n")
aggregated_individuals = generate_joint_distribution(marginal_ind_sex_maritalstatus, marginal_ind_income, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file.json")
print(sum(aggregated_individuals.:population), "\n")
aggregated_individuals = generate_joint_distribution(marginal_ind_income, marginal_ind_sex_maritalstatus, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file.json")
=#

#get the dataframes used for allocation
aggregated_individuals = generate_joint_distribution(marginal_ind_income, marginal_ind_sex_maritalstatus, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file.json")
aggregated_households = generate_joint_distribution(marginal_hh_size)

#old function for assignment
#disaggregated_households, unassigned1 = assign_individuals_to_households(aggregated_individuals, aggregated_households, return_unassigned = true)

HOUSEHOLD_SIZE_COLUMN = :hh_size
POPULATION_COLUMN = :population
ID_COLUMN = :id
AGE_COLUMN = :age
MARITALSTATUS_COLUMN = :maritalstatus
SEX_COLUMN = :sex
MINIMUM_ADULT_AGE = 23
AVAILABLE_FOR_MARRIAGE = "Married"

#new assignment with optimization 
model, allocation_values, disaggregated_individuals, disaggregated_households = assign_and_optimize_individuals_to_households!(aggregated_individuals, aggregated_households)

print(disaggregated_individuals)