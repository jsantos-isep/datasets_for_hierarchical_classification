import os

import numpy as np
import pandas as pd
from pandas import json_normalize
from dotenv import load_dotenv
from natsort import natsorted

load_dotenv()


# Function to get the highest scoring topic different from primary
def get_highest_alternate_topic(topics, primary):
    # Convert list of topics to DataFrame
    topics_df = pd.DataFrame(topics)

    # Filter out the primary topic
    topics_df = topics_df[topics_df["display_name"] != primary]

    # Select the topic with the highest score and convert to lowercase
    if not topics_df.empty:
        return topics_df.sort_values(by="score", ascending=False).iloc[0]["display_name"].lower().replace(" ", "_")
    return "others"

# Function to reconstruct text
def reconstruct_text(index_dict):
    if not isinstance(index_dict, dict):  # Check if it's a valid dictionary
        return np.nan  # Return an empty string for None values

    words_with_positions = [(pos, word) for word, positions in index_dict.items() for pos in positions]
    sorted_words = sorted(words_with_positions)  # Sort by position
    return " ".join(word for _, word in sorted_words)  # Join words into a sentence


def pre_processing():
    in_directory = os.getenv("DATASETS_FOLDER_BIG_OPENALEX_ORIGINAL_JSON")
    out_directory = os.getenv("DATASETS_FOLDER_BIG_OPENALEX_ORIGINAL_CSV")
    for filename in sorted(os.listdir(in_directory)):
        if filename.endswith(".json") and not filename.startswith("documents_2022"):
            f = os.path.join(in_directory, filename)
            output_file = os.path.join(out_directory, f"{os.path.splitext(filename)[0]}.csv")
            df = pd.read_json(f)
            df = df[df["language"] == "en"]
            # Apply function to create text column
            df["abstract"] = df["abstract_inverted_index"].apply(reconstruct_text)
            df['text'] = df['display_name'].astype(str) + ' ' + df['abstract'].astype(str)
            df["category"] = df["primary_topic"].apply(
                lambda x:  x["field"]["display_name"].lower().replace(",", "").replace(" ", "_") if isinstance(x, dict) else None)
            df["sub_category"] = df["primary_topic"].apply(
                lambda x: (x["subfield"]["display_name"]).strip().lower().replace(" ", "_") if isinstance(x, dict) else None
            )
                #df.apply(
                #lambda row: get_highest_alternate_topic(row["topics"], row["primary_topic"]["display_name"]), axis=1))
            df['sub_category'] = df['category'] + '.' + df['sub_category']

            columns_to_drop = ["id", "doi", "ids", "display_name", "type_crossref", "institution_assertions", "authorships",
                          "open_access", "primary_location", "publication_year", "institutions_distinct_count",
                          "corresponding_author_ids", "corresponding_institution_ids", "countries_distinct_count", "concepts",
                          "indexed_in", "apc_list", "apc_paid", "fwci", "has_fulltext", "cited_by_count", "citation_normalized_percentile",
                            "cited_by_percentile_year", "biblio", "is_retracted", "is_paratext", "primary_topic",
                          "locations_count", "locations", "best_oa_location", "referenced_works_count", "versions", "keywords",
                          "abstract_inverted_index_v3", "related_works", "cited_by_api_url", "abstract_inverted_index",
                          "counts_by_year", "updated_date", "created_date", "is_authors_truncated", "referenced_works", "language",
                          "mesh", "sustainable_development_goals", "grants", "fulltext_origin", "datasets", "topics", "type"]

            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
            df = df.dropna(subset=['title', 'abstract'])
            df.to_csv(output_file, sep=";", index=False)

def merge_files():
    input_dir = os.getenv("DATASETS_FOLDER_BIG_OPENALEX_ORIGINAL_CSV")
    output_dir = os.getenv("DATASETS_FOLDER_BIG_OPENALEX")
    output_file = os.path.join(output_dir, "big_openalex.csv")
    print(natsorted(os.listdir(input_dir)))
    lines = []
    for filename in natsorted(os.listdir(input_dir)):
        if filename.endswith(".csv"):
            lines.append(pd.read_csv(os.path.join(input_dir, filename), delimiter=";"))
    df_res = pd.concat(lines, ignore_index=True)
    df_sorted = df_res.sort_values(by="publication_date")
    df_sorted.to_csv(output_file, sep=";", index=False)

if __name__ == "__main__":
    pre_processing()
    merge_files()