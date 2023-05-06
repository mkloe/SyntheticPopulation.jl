using GeoJSON, Downloads, TranscodingStreams, CodecZlib


function read_geojson_file(path::String)
    jsonbytes = read(path)
    fc = GeoJSON.read(jsonbytes)
    areas = DataFrame(fc)[:,[1,6]]
    id = collect(1:nrow(areas))
    insertcols!(areas, 1, ID_COLUMN => id)
    return areas
end


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


function generate_areas_dataframe(url::String, target_filepath::String = pwd())
    filepath = download_osm_boundaries(url, target_filepath)
    areas = read_geojson_file(filepath)
    rm(filepath)
    return areas
end