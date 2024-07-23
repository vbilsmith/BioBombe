import os
import json
import pandas as pd
import grein_loader as loader

def load_log(logFile):
    log = pd.read_csv(logFile, names=["GSE", "status"])
    log = log[log["status"] == "end"]
    return set(log["GSE"].unique())

def load_data(geo_accession, count_matrix_df, greinMetadata):
    description, metadata, count_matrix = loader.load_dataset(geo_accession)
    for gsm, valDict in metadata.items():
        valDict.drop(" ")
        valDict.drop("geo_accession")
    # Merge in to existing count matrix DF
    if count_matrix_df.empty:
        return count_matrix, {geo_accession: metadata}
    else:
        greinMetadata[geo_accession] = metadata
        return pd.merge(count_matrix_df, count_matrix, how='left', on=["gene", "gene_symbol"]), greinMetadata


filenames = dict()
data = dict()
fout = dict()
for species in ["human", "mouse", "rat"]:
    print(species)
    # Check if there is already expression data downloaded or whether we are starting fresh
    filenames[species] = dict()
    filenames[species]["log"] = "greinLoad_{0}.log".format(species)
    filenames[species]["data"]= "../../download/grein_count_matrix_{0}.pkl".format(species)
    filenames[species]["metadata"] = "../../download/grein_metadata_{0}.json".format(species)

    data[species] = dict()
    if os.path.exists(filenames[species]["log"]) and os.path.exists(filenames[species]["data"]) and os.path.exists(filenames[species]["metadata"]):
        print("...loading existing {0} data".format(species))
        data[species]["count_matrix"] = pd.read_pickle(filenames[species]["data"])
        data[species]["sample_data"] = json.load(open(filenames[species]["metadata"], "r"))
        data[species]["completed"] = load_log(filenames[species]["log"])

    else:
        print("starting {0} download from scratch".format(species))
        os.system(f"rm {filenames[species]['log']}")
        data[species]["count_matrix"] = pd.DataFrame()
        data[species]["sample_data"] = dict()
        data[species]["completed"] = set()
        for ftype in ["log", "metadata"]:
            fname = filenames[species][ftype]
            os.system(f"touch {fname}")

    fout[species] = open(filenames[species]["metadata"], "a")

# Saving human gene expression data
species_df = pd.read_csv("../../download/species.csv")

for species in ["human", "mouse", "rat"]:
    GSEs = species_df[species_df["species"] == species]
    GSEs = GSEs[~GSEs["accession"].isin(data["human"]["completed"])]["accession"]
    
    logfile = open(filenames[species]["log"], 'a')
    count_matrix = data[species]["count_matrix"]
    sample_data = data[species]["sample_data"]
    
    for geo_accession in GSEs:
        print(geo_accession)
        # If new data, download data and add to df
        logfile.write(geo_accession + ",start\n")
    
        count_matrix, sample_data = load_data(geo_accession, count_matrix, sample_data)
    
        count_matrix.to_pickle(count_matrix)
        json.dump(sample_data, open(sample_data, "w"))
        logfile.write(geo_accession + ",end\n")

        break
