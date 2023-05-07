push!(LOAD_PATH,"../src/")
using Documenter, SyntheticPopulation

makedocs(sitename="My Documentation", modules = [SyntheticPopulation])