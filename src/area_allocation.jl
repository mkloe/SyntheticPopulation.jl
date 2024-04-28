"""
    get_coordinates(geometry)

Auxilary function - it returns array of coordinates that define the boundaries of a polygon.

Arguments:
- `geometry` - a Polygon or Multipolygon object
"""
function get_coordinates(geometry)
    if typeof(geometry) == GeoJSON.Polygon{2,Float32}
        coordinates = geometry[1]
    elseif typeof(geometry) == GeoJSON.MultiPolygon{2,Float32}
        #if it is multipolygon it is okay to concatenate, because we only use these values to pick max&min lat, lon.
        coordinates = map(i -> geometry[i][1], collect(1:length(geometry)))
        coordinates = reduce(vcat, coordinates)
    end
    return coordinates
end


"""
    find_point_in_polygon(area::DataFrameRow)

Auxilary function - it returns a random point in a polygon that is stored in a data frame row.

Arguments:
- `area` - a data frame row that includes a column `:geometry` with Polygon or Multipolygon object
"""
function find_point_in_polygon(area::DataFrameRow)
    lon = rand(Uniform(area.:min_lon, area.:max_lon))
    lat = rand(Uniform(area.:min_lat, area.:max_lat))

    if typeof(area.:geometry) == GeoJSON.Polygon{2,Float32}
        while true
            inpolygon((lon, lat), area.:geometry[1]) == 1 && return lon, lat
            lon = rand(Uniform(area.:min_lon, area.:max_lon))
            lat = rand(Uniform(area.:min_lat, area.:max_lat))
        end
    elseif typeof(area.:geometry) == GeoJSON.MultiPolygon{2,Float32}
        while true
            for i = 1:length(area.:geometry)
                inpolygon((lon, lat), area.:geometry[i][1]) == 1 && return lon, lat
            end
            lon = rand(Uniform(area.:min_lon, area.:max_lon))
            lat = rand(Uniform(area.:min_lat, area.:max_lat))
        end
    end
end


"""
    assign_areas_to_households!(disaggregated_households::DataFrame, aggregated_households::DataFrame, aggregated_areas::DataFrame; return_unassigned::Bool = false)

Main function - mutates the data frame `disaggregated_households` by assigning the areas and coordinates to each household.

Arguments:
- `disaggregated_households` - data frame that represents the generated population of disaggregated households. More information specified in `notebooks/dataframe_formats.ipynb`
- `aggregated_idividuals` - data frame of aggregated individuals. More information specified in `notebooks/dataframe_formats.ipynb`
- `aggregated_idividuals` - data frame of aggregated areas. More information specified in `notebooks/dataframe_formats.ipynb`
- `return_unassigned` - an optional argument indicating whether the data frames with individuals and households that were not assigned should be returned.
"""
function assign_areas_to_households!(
    disaggregated_households::DataFrame,
    aggregated_households::DataFrame,
    aggregated_areas::DataFrame;
    return_unassigned::Bool = false,
)
    print(
        "===================\nAssigning coordinates to households...\n===================\n",
    )

    disaggregated_households.:hh_size = map(
        x -> aggregated_households[x, HOUSEHOLD_SIZE_COLUMN],
        disaggregated_households.:hh_attr_id,
    )

    aggregated_areas_df = copy(aggregated_areas)
    input_disaggregated_households = copy(disaggregated_households)

    #filter out households that were not allocated
    disaggregated_households =
        filter(:individuals_allocated => ==(true), disaggregated_households)

    #add lat and lon columns column to the household
    input_disaggregated_households.:lat = zeros(nrow(input_disaggregated_households))
    input_disaggregated_households.:lon = zeros(nrow(input_disaggregated_households))
    input_disaggregated_households.:area_id =
        Int.(zeros(nrow(input_disaggregated_households)))
    disaggregated_households.:lat = zeros(nrow(disaggregated_households))
    disaggregated_households.:lon = zeros(nrow(disaggregated_households))
    disaggregated_households.:area_id = Int.(zeros(nrow(disaggregated_households)))

    #select max lat and max lon for each of the districts.
    coordinates = map(get_coordinates, aggregated_areas_df.:geometry)
    aggregated_areas_df.:max_lon =
        map(district -> maximum(map(lonlat -> lonlat[1], district)), coordinates)
    aggregated_areas_df.:min_lon =
        map(district -> minimum(map(lonlat -> lonlat[1], district)), coordinates)
    aggregated_areas_df.:max_lat =
        map(district -> maximum(map(lonlat -> lonlat[2], district)), coordinates)
    aggregated_areas_df.:min_lat =
        map(district -> minimum(map(lonlat -> lonlat[2], district)), coordinates)

    @showprogress for _ = 1:nrow(disaggregated_households)

        #select household that does not have any lat lon assigned yet
        disaggregated_households = filter(:lat => ==(0), disaggregated_households)
        disaggregated_households = filter(:lon => ==(0), disaggregated_households)
        nrow(disaggregated_households) == 0 ? break : nothing
        household_row_number = sample(collect(1:nrow(disaggregated_households)))
        household_id = disaggregated_households[household_row_number, ID_COLUMN]

        #select random area
        aggregated_areas_df_filtered = filter(
            POPULATION_COLUMN =>
                >=(disaggregated_households[household_row_number, :hh_size]),
            aggregated_areas_df,
        )
        nrow(aggregated_areas_df_filtered) == 0 ? break : nothing
        area_row_number = sample(
            collect(1:nrow(aggregated_areas_df_filtered)),
            Weights(aggregated_areas_df_filtered[:, POPULATION_COLUMN]),
        )
        area_id = aggregated_areas_df_filtered[area_row_number, ID_COLUMN]

        #select lon and lat within the district
        area = aggregated_areas_df_filtered[area_row_number, :]
        lon, lat = find_point_in_polygon(area)

        #assign lat and lon
        input_disaggregated_households[household_id, :lon] = lon
        input_disaggregated_households[household_id, :lat] = lat
        input_disaggregated_households[household_id, :area_id] = area_id
        disaggregated_households[household_row_number, :lon] = lon
        disaggregated_households[household_row_number, :lon] = lon
        disaggregated_households[household_row_number, :area_id] = area_id

        #subtract houshold size from target population for the area
        aggregated_areas_df[area_row_number, POPULATION_COLUMN] -=
            disaggregated_households[household_row_number, :hh_size]

    end

    disaggregated_households = filter(:lat => ==(0), disaggregated_households)
    disaggregated_households = filter(:lon => ==(0), disaggregated_households)

    #return data
    input_disaggregated_households = input_disaggregated_households[:, Not(:hh_size)]
    aggregated_areas_df = filter(POPULATION_COLUMN => >(1), aggregated_areas_df)
    aggregated_areas_df =
        aggregated_areas_df[:, [ID_COLUMN, :geometry, :name_en, POPULATION_COLUMN]]
    aggregated_areas_df[:, POPULATION_COLUMN] =
        Int.(aggregated_areas_df[:, POPULATION_COLUMN])

    if return_unassigned == true
        unassigned = Dict{String,DataFrame}()
        unassigned["disaggregated_unassigned_households"] = disaggregated_households
        unassigned["unassigned_areas"] = aggregated_areas_df

        return input_disaggregated_households, unassigned
    else
        return input_disaggregated_households
    end
end
