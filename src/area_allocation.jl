function get_coordinates(geometry)
    if typeof(geometry) == GeoJSON.Polygon{JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}}
        coordinates = geometry[1]
    elseif typeof(geometry) == GeoJSON.MultiPolygon{JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}}
        #if it is multipolygon it is okay to concatenate, because we only use these values to pick max&min lat, lon.
        coordinates = map(i -> geometry[i][1], collect(1:length(geometry)))
        coordinates = reduce(vcat, coordinates)
    end
    return coordinates
end


function find_point_in_polygon(area::DataFrameRow, lon::Float64, lat::Float64)
    if typeof(area.:geometry) == GeoJSON.Polygon{JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}}
        while true
            inpolygon((lon, lat), area.:geometry[1]) == 1 && return lon, lat
            lon = rand(Uniform(area.:min_lon, area.:max_lon))
            lat = rand(Uniform(area.:min_lat, area.:max_lat))
        end
    elseif typeof(area.:geometry) == GeoJSON.MultiPolygon{JSON3.Object{Vector{UInt8}, SubArray{UInt64, 1, Vector{UInt64}, Tuple{UnitRange{Int64}}, true}}}
        while true
            for i in 1:length(area.:geometry)
                inpolygon((lon, lat), area.:geometry[i][1]) == 1 && return lon, lat
            end
            lon = rand(Uniform(area.:min_lon, area.:max_lon))
            lat = rand(Uniform(area.:min_lat, area.:max_lat))
        end
    end
end


function assign_areas_to_households!(disaggregated_households::DataFrame, aggregated_areas::DataFrame; return_unassigned::Bool = false)
    print("===================\nAssigning coordinates to households...\n===================\n")
    aggregated_areas_df = copy(aggregated_areas)
    input_disaggregated_households = copy(disaggregated_households)

    #filter out households that were not allocated
    disaggregated_households = filter(:individuals_allocated => ==(true), disaggregated_households)

    #add lat and lon columns column to the household
    input_disaggregated_households.:lat = zeros(nrow(input_disaggregated_households))
    input_disaggregated_households.:lon = zeros(nrow(input_disaggregated_households))
    input_disaggregated_households.:area_id = Int.(zeros(nrow(input_disaggregated_households)))
    disaggregated_households.:lat = zeros(nrow(disaggregated_households))
    disaggregated_households.:lon = zeros(nrow(disaggregated_households))
    disaggregated_households.:area_id = Int.(zeros(nrow(disaggregated_households)))

    #select max lat and max lon for each of the districts.
    coordinates = map(get_coordinates, aggregated_areas_df.:geometry)
    aggregated_areas_df.:max_lon = map(district -> maximum(map(lonlat -> lonlat[1], district)), coordinates)
    aggregated_areas_df.:min_lon = map(district -> minimum(map(lonlat -> lonlat[1], district)), coordinates)
    aggregated_areas_df.:max_lat = map(district -> maximum(map(lonlat -> lonlat[2], district)), coordinates)
    aggregated_areas_df.:min_lat = map(district -> minimum(map(lonlat -> lonlat[2], district)), coordinates)

    @showprogress for _ in 1:nrow(disaggregated_households)
        
        #select household that does not have any lat lon assigned yet
        disaggregated_households = filter(:lat => ==(0), disaggregated_households)
        disaggregated_households = filter(:lon => ==(0), disaggregated_households)
        nrow(disaggregated_households) == 0 ? break : nothing
        household_row_number = sample(collect(1:nrow(disaggregated_households)))
        household_id = disaggregated_households[household_row_number, :id]

        #select random area
        aggregated_areas_df_filtered = filter(:population => >=(disaggregated_households[household_row_number, HOUSEHOLD_SIZE_COLUMN]), aggregated_areas_df)
        nrow(aggregated_areas_df_filtered) == 0 ? break : nothing
        area_row_number = sample(collect(1:nrow(aggregated_areas_df_filtered)), Weights(aggregated_areas_df_filtered.:population))
        area_id = aggregated_areas_df_filtered[area_row_number, :id]
        
        #select lon and lat within the district
        area = aggregated_areas_df_filtered[area_row_number,:]
        lon = rand(Uniform(area.:min_lon, area.:max_lon))
        lat = rand(Uniform(area.:min_lat, area.:max_lat))
        lon, lat = find_point_in_polygon(area, lon, lat)
        
        #assign lat and lon
        input_disaggregated_households[household_id, :lon] = lon
        input_disaggregated_households[household_id, :lat] = lat
        input_disaggregated_households[household_id, :area_id] = area_id
        disaggregated_households[household_row_number, :lon] = lon
        disaggregated_households[household_row_number, :lon] = lon
        disaggregated_households[household_row_number, :area_id] = area_id

        #subtract houshold size from target population for the area
        aggregated_areas_df[area_row_number, :population] -= disaggregated_households[household_row_number, HOUSEHOLD_SIZE_COLUMN]

    end
    
    disaggregated_households = filter(:lat => ==(0), disaggregated_households)
    disaggregated_households = filter(:lon => ==(0), disaggregated_households)

    #return data
    input_disaggregated_households = input_disaggregated_households[:, Not(HOUSEHOLD_SIZE_COLUMN)] 
    aggregated_areas_df = filter(:population => >(1), aggregated_areas_df)
    aggregated_areas_df = aggregated_areas_df[:, [:id, :geometry, :name, :population]]
    aggregated_areas_df.:population = Int.(aggregated_areas_df[:, :population])

    if return_unassigned == true
        unassigned = Dict{String, DataFrame}()
        unassigned["disaggregated_unassigned_households"] = disaggregated_households
        unassigned["unassigned_areas"] = aggregated_areas_df

        return input_disaggregated_households, unassigned
    else
        return input_disaggregated_households
    end
end
