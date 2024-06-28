using DataFrames
using SyntheticPopulation

#each individual and each household represent 100.000 individuals or households
SCALE = 0.00001 

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


#areas
URL = "https://osm-boundaries.com/Download/Submit?apiKey=ba9912041675bdf0cb40ca82631b6e96&db=osm20230102&osmIds=-2988894,-2988933,-2988895,-288600,-2988896,-2988946,-5505984,-2988897,-2988898,-2988899,-2988900,-5505985,-2988901,-2988902,-568660,-2988903&format=GeoJSON&srid=4326"
areas = generate_areas_dataframe_from_url(URL)

#aggregated_areas - population referenced from https://nj.tjj.beijing.gov.cn/nj/main/2021-tjnj/zk/indexeh.htm
aggregated_areas = copy(areas)
aggregated_areas.:population = SCALE .* 10000 .* [56.8, 313.2, 201.9, 345.1, 34.6, 184.0, 132.4, 45.7, 52.8, 39.3, 44.1, 131.3, 199.4, 226.9, 110.6, 70.9]
aggregated_areas

#generation of dataframe of individuals
aggregated_individuals = generate_joint_distribution(marginal_ind_sex_maritalstatus, marginal_ind_income, marginal_ind_age_sex, config_file = "tutorial_notebooks/config_file.json")

#generation of dataframe of households
aggregated_households = generate_joint_distribution(marginal_hh_size)

#mo = assign_and_optimize_individuals_to_households(aggregated_individuals, aggregated_households)

model, allocation_values, disaggregated_individuals, disaggregated_households = assign_and_optimize_individuals_to_households(aggregated_individuals, aggregated_households)