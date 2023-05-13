using Distributions
using DataFrames

function validate_table(estimated_df, target_df)
    
    #prepare data frames for computation
    sort!(estimated_df)
    sort!(target_df)
    rename!(estimated_df, :population => :estimated_population)
    attribute_names = Symbol.(intersect(names(estimated_df), names(target_df)))
    target_df = leftjoin(target_df, estimated_df, on = attribute_names, matchmissing = :equal)
    #replace missing with zeroes
    missing_indices = findall(ismissing.(target_df[:, :estimated_population]))
    target_df[missing_indices, :estimated_population] .= 0

    #define data needed for formula
    p = target_df.:population/sum(target_df.:population)
    t = target_df.:estimated_population/sum(target_df.:population)
    N = sum(target_df.:population)

    #compute Z score
    target_df.:Z_score = (p .- t) ./ sqrt.((p .* (1 .- p)) ./ N)
    target_df.:Z_score = map(x -> isnan(x) ? 0 : x, target_df.:Z_score)
    
    #assess cells
    wfv_095 = count(i -> (-1.96<i<1.96), target_df.Z_score) / nrow(target_df)
    wfv_090 = count(i -> (-1.645<i<1.645), target_df.Z_score) / nrow(target_df)
    print("=================\n")
    print("=Cell statistics=\n")
    print("=================\n\n")
    print("Percentage of well fitting values at 0.95 confidence interval: ", wfv_095, "\n")
    print("Percentage of well fitting values at 0.90 confidence interval: ", wfv_090, "\n\n\n")

    #assess whole table
    degrees_of_freedom = nrow(target_df)
    distribution = Chisq(degrees_of_freedom)
    critical_value_090 = quantile(distribution, 0.90)
    critical_value_095 = quantile(distribution, 0.95)
    cv = sum(target_df.Z_score .^ 2)

    print("==================\n")
    print("=Table statistics=\n")
    print("==================\n\n")
    if (cv < critical_value_090)
        print("Statistic value equals: ", cv, "\n")
        print("Table is well fitting at 0.9 and 0.95 confidence interval.\n")
    elseif (critical_value_090 < cv < critical_value_095)
        print("Statistic value equals: ", cv, "\n")
        print("Table is well fitting at 0.95 but not well fitting at 0.90 confidence interval."\n)
    else 
        print("Statistic value equals: ", cv, "\n")
        print("Table is not well fitting.\n")
    end
    
    return target_df
end


function compute_marginals(estimated_df)
    #Population by age and sex
    age_sex = combine(groupby(estimated_df, [:AGE, :SEX], sort=true), :estimated_population => sum); 
    sort!(estimated_df, [:SEX, :AGE])

    #Population by sex and marital status
    sex_marital = combine(groupby(estimated_df, [:MARITAL_STATUS, :SEX], sort=true), :estimated_population => sum); 
    sort!(estimated_df, [:SEX, :MARITAL_STATUS])

    #Population by income
    income = combine(groupby(estimated_df, [:INCOME], sort=true), :estimated_population => sum)

    #Correct column names
    age_sex, sex_marital, income = map(x -> rename!(x, :estimated_population_sum => :population), [age_sex, sex_marital, income])

    #filter out missing values
    sex_marital = filter(:MARITAL_STATUS => x -> typeof(x) != Missing, sex_marital)
    income = filter(:INCOME => x -> typeof(x) != Missing, income)

    return age_sex, sex_marital, income
end