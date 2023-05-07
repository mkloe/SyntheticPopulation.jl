push!(LOAD_PATH,"../src/")
using Documenter, SyntheticPopulation

makedocs(sitename="Documentation", modules = [SyntheticPopulation])