"""
    read_geojson_file(path::String)

Auxilary function - it reads the GeoJSON file and returns the data in data frame format.

Arguments:
- `path` - path to the GeoJSON file.
"""
function read_geojson_file(path::String)
    jsonbytes = read(path)
    fc = GeoJSON.read(jsonbytes)
    areas = DataFrame(fc)
    areas = areas[:, [:name_en, :geometry]]
    id = collect(1:nrow(areas))
    insertcols!(areas, 1, ID_COLUMN => id)
    return areas
end


"""
    download_osm_boundaries(url::String, target_filepath::String = pwd())

Auxilary function - downloads a GeoJSON file with boundaries and returns its filepath.

Arguments:
- `url` - URL generated from https://osm-boundaries.com/ 
- `target_filepath` - target path of the downloaded file
"""
function download_osm_boundaries(url::String, target_filepath::String = pwd())

    #download file
    print("Downloading file... \n")
    filepath = joinpath(target_filepath, "file.gz")
    Downloads.download(url, filepath)

    #unzip file
    print("File downloaded. Unzipping file...\n")
    target_filepath = joinpath(target_filepath, "file.geojson")
    open(filepath, "r") do f
        s = TranscodingStream(GzipDecompressor(), f)
        open(target_filepath, "w") do out
            write(out, s)
        end
    end

    #delete zipped file
    rm(filepath)
    print("File saved at ", target_filepath, "\n")
    return target_filepath

end


"""
    generate_areas_dataframe_from_url(url::String, target_filepath::String = pwd())

Main function - it returns a generated data frame with areas and its attributes given a URL address.

Arguments:
- `url` - the URL with selected areas generated from https://osm-boundaries.com/
- `target_filepath` - target path of the downloaded file
"""
function generate_areas_dataframe_from_url(url::String, target_filepath::String = pwd())
    filepath = download_osm_boundaries(url, target_filepath)
    areas = read_geojson_file(filepath)
    rm(filepath)
    return areas
end

"""
    generate_areas_dataframe_from_file(filepath::String)

Main function - it returns a generated data frame with areas and its attributes given a file path.

Arguments:
- `filepath` - path to the file in .geojson format
"""
function generate_areas_dataframe_from_file(filepath::String)
    areas = read_geojson_file(filepath)
    return areas
end
