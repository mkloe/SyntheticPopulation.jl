#each individual and each household represent 1000 individuals or households
SCALE = 0.001 

#all values are based on China census data
popoulation_size = 21890000

marginal_ind_age_sex = DataFrame(
    sex = repeat(['M', 'F'], 18),
    age = repeat(2:5:87, inner = 2), 
    population = SCALE .* 10000 .* [52.6, 49.0, 48.5, 44.8, 33.6, 30.6, 34.6, 28.8, 71.6, 63.4, 99.6, 90.9, 130.9, 119.4, 110.8, 103.5, 83.8, 76.4, 84.2, 77.7, 84.2, 77.8, 82.8, 79.9, 67.7, 71.0, 56.9, 62.6, 31.5, 35.3, 18.5, 23.0, 15.2, 19.7, 12.5, 16.0]
    )


marginal_ind_sex_maritalstatus = DataFrame(
    maritalstatus = repeat(["Not_married", "Married", "Divorced", "Widowed"], 2),
    sex = repeat(['M', 'F'], inner = 4),  
    population = SCALE .* [1679, 5859, 140, 128, 1611, 5774, 206, 426] ./ 0.00082
    )


marginal_ind_income = DataFrame(
    income = [25394, 44855, 63969, 88026, 145915], 
    population = repeat([popoulation_size * SCALE / 5], 5)
    )


marginal_hh_size = DataFrame(
    hh_size = [1,2,3,4,5],
    population = Int.(round.(SCALE * 8230000 .* [0.299, 0.331, 0.217, 0.09, 0.063]))
    )