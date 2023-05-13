include("../src/SyntheticPopulation.jl")
include("utils.jl")
using .SyntheticPopulation
using DataFrames
using StatsBase
SIZE = 300000
OLD_ADULTS = 0.6
YOUNG_ADULTS = 0.2
CHILDREN = 0.2

SEX = ['M', 'F']; SEX_WEIGHTS = [0.5, 0.5]
MARITAL_STATUS = ["Not_married", "Married", "Divorced", "Widowed"]; 
MARITAL_WEIGHTS = [0.3, 0.5, 0.1, 0.1]

AGE_YOUNG_ADULT = [20, 25];
AGE_YOUNG_ADULT_WEIGHTS = repeat([1 / length(AGE_YOUNG_ADULT)], length(AGE_YOUNG_ADULT));
AGE_OLD_ADULT = [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80];
AGE_OLD_ADULT_WEIGHTS = repeat([1 / length(AGE_OLD_ADULT)], length(AGE_OLD_ADULT));
AGE_CHILDREN = [5, 10, 15];
AGE_CHILDREN_WEIGHTS = repeat([1 / length(AGE_CHILDREN)], length(AGE_CHILDREN));

INCOME = [40000, 50000, 60000, 70000, 80000];
ZERO_INCOME = [60000, 70000, 80000];
INCOME_WEIGHTS_YOUNG = SIZE .* YOUNG_ADULTS .* [0.5, 0.5, 0, 0, 0]
INCOME_WEIGHTS_OLD = SIZE * (OLD_ADULTS+YOUNG_ADULTS) .* [0.2, 0.2, 0.2, 0.2, 0.2] .- INCOME_WEIGHTS_YOUNG

population_young_adult = DataFrame(
    AGE = sample(AGE_YOUNG_ADULT, Weights(AGE_YOUNG_ADULT_WEIGHTS), Int(SIZE * YOUNG_ADULTS)),
    MARITAL_STATUS = sample(MARITAL_STATUS, Weights(MARITAL_WEIGHTS), Int(SIZE * YOUNG_ADULTS)),
    SEX = sample(SEX, Weights(SEX_WEIGHTS), Int(SIZE * YOUNG_ADULTS)),
    INCOME = sample(INCOME, Weights(INCOME_WEIGHTS_YOUNG), Int(SIZE * YOUNG_ADULTS)),
)
population_old_adult = DataFrame(
    AGE = sample(AGE_OLD_ADULT, Weights(AGE_OLD_ADULT_WEIGHTS), Int(SIZE * OLD_ADULTS)),
    MARITAL_STATUS = sample(MARITAL_STATUS, Weights(MARITAL_WEIGHTS), Int(SIZE * OLD_ADULTS)),
    SEX = sample(SEX, Weights(SEX_WEIGHTS), Int(SIZE * OLD_ADULTS)),
    INCOME = sample(INCOME, Weights(INCOME_WEIGHTS_OLD), Int(SIZE * OLD_ADULTS)),
)
population_children = DataFrame(
    AGE = sample(AGE_CHILDREN, Weights(AGE_CHILDREN_WEIGHTS), Int(SIZE * CHILDREN)),
    MARITAL_STATUS = repeat([missing], Int(SIZE * CHILDREN)),
    SEX = sample(SEX, Weights(SEX_WEIGHTS), Int(SIZE * CHILDREN)),
    INCOME = repeat([missing], Int(SIZE * CHILDREN))
)

disaggregated_independent_population = reduce(vcat, [
    population_young_adult, 
    population_old_adult,
    population_children
    ]
)

independent_population = combine(groupby(disaggregated_independent_population, names(disaggregated_independent_population), sort=true), nrow)
rename!(independent_population, :nrow => :population)
zero_population = DataFrame(vec(collect(Iterators.product(AGE_YOUNG_ADULT, MARITAL_STATUS, SEX, ZERO_INCOME))))
zero_population.:population = repeat([0], nrow(zero_population))
rename!(zero_population, names(independent_population))
independent_population = reduce(vcat, [independent_population, zero_population])

#Population by age and sex
independent_age_sex = combine(groupby(disaggregated_independent_population, [:AGE, :SEX], sort=true), nrow); 
sort!(independent_age_sex, [:SEX, :AGE])

#Population by sex and marital status
independent_sex_marital = combine(groupby(disaggregated_independent_population, [:MARITAL_STATUS, :SEX], sort=true), nrow); 
sort!(independent_sex_marital, [:SEX, :MARITAL_STATUS])

#Population by income
independent_income = combine(groupby(disaggregated_independent_population, [:INCOME], sort=true), nrow)

#Correct column names
independent_age_sex, independent_sex_marital, independent_income = map(x -> rename!(x, :nrow => :population), [independent_age_sex, independent_sex_marital, independent_income])

#filter out missing values
independent_sex_marital = filter(:MARITAL_STATUS => x -> typeof(x) != Missing, independent_sex_marital)
independent_income = filter(:INCOME => x -> typeof(x) != Missing, independent_income)


modified = SyntheticPopulation.generate_joint_distribution(independent_age_sex, independent_sex_marital, independent_income, config_file = "evaluation_notebooks/ind_modified.json")
#modified = modified[:, Not(:id)]

res = validate_table(modified, independent_population)
modified_age_sex, modified_sex_marital, modified_income = compute_marginals(modified)
validate_table(modified_income, independent_income)