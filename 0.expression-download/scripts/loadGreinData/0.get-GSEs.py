import os
import random
import json
import ast
import pandas as pd
import grein_loader as loader

def load_log(logFile):
    log = pd.read_csv(logFile, names=["GSE", "status"])
    log = log[log["status"] == "end"]
    return set(log["GSE"].unique())

filenames, data, fout = {}, {}, {}

for species in ["human", "mouse", "rat"]:
    print(species)
    # Check if there is already expression data downloaded or whether we are starting fresh
    filenames[species] = dict()
    filenames[species]["log"] = f'greinLoad_{species}.log'
    filenames[species]["data"] = f'../../download/grein_count_matrix_{species}.pkl'
    filenames[species]["metadata"] = f'../../download/grein_metadata_{species}.json'
    filenames[species]["description"] = f'../../download/grein_descriptions_{species}.json'

    data[species] = dict()
    if os.path.exists(filenames[species]["log"]) and os.path.exists(
            filenames[species]["data"]) and os.path.exists(filenames[species]["metadata"]):
        print(f"...loading existing {species} data")
        data[species]["count_matrix"] = pd.read_pickle(filenames[species]["data"])
        data[species]["sample_data"] = json.load(open(filenames[species]["metadata"], "r"))
        data[species]["completed"] = load_log(filenames[species]["log"])

        # Load description data, which is messier
        desc = open(filenames[species]["description"], "r")
        descdata = desc.readlines()
        data[species]["study_desc"] = {}
        for entry in descdata:
            entry = ast.literal_eval(entry)
            accession = entry['geo_accession']
            data[species]["study_desc"][accession] = entry

    else:
        print(f"starting {species} download from scratch")
        os.system(f"rm {filenames[species]['log']}")
        data[species]["count_matrix"] = pd.DataFrame()
        data[species]["sample_data"] = {}
        data[species]["completed"] = set()
        for ftype in ["log", "metadata", "description"]:
            fname = filenames[species][ftype]
            os.system(f"touch {fname}")

    fout[species] = {}
    fout[species]["sample_data"] = open(filenames[species]["metadata"], "a")
    fout[species]["descriptions"] = open(filenames[species]["description"], "a")

# Accessing the geo_accession ID and study species from overview
geo_accession_ids = []
species_decode = {"Homo sapiens": "human", "Mus musculus": "mouse", "Rattus norvegicus": "rat"}

if os.path.isfile("../../download/species.csv"):
    species_df = pd.read_csv("../../download/species.csv").set_index("accession")
    species_list = dict(zip(species_df.index, list(species_df["species"])))
else:
    species_list = {}

overview = loader.load_overview()

for desc in overview:
    geo_accession_id = desc['geo_accession']
    if geo_accession_id in species_list.keys():
        print(f"skipping {geo_accession_id}")
    else:
        species_latin = desc['species']
        if species_latin not in species_decode:
            print(geo_accession_id, species_latin)
        else:
            species = species_decode[species_latin]
            # if geo_accession_id not in data[species]["study_desc"].keys():
            fout[species]["descriptions"].write(str(desc) + "\n")

            # geo_accession_ids.append(geo_accession_id)
            species_list[geo_accession_id] = species
    
    if random.randint(0, 20) == 0:
        GSE_species_df = pd.DataFrame.from_dict(species_list, orient='index', columns=["species"])
        print(f"Backing up species table, current count is {GSE_species_df.shape[0]}")
        GSE_species_df.to_csv("../../download/species.csv", index_label="accession")

# saving the species in each GSE
GSE_species_df = pd.DataFrame.from_dict(species_list, orient='index', columns=["species"])
GSE_species_df.to_csv("../../download/species.csv", index_label="accession")
