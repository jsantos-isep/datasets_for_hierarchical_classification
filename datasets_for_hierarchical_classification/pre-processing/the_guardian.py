import os
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from natsort import natsorted
import re
from sklearn.preprocessing import LabelEncoder


load_dotenv()

def remove_html_tags(text):
    if pd.isna(text):  # Handle NaN values
        return ""
    text = BeautifulSoup(text, "html.parser").get_text().replace("\n", " ").strip()
    return re.sub(r'\s+', ' ', text).strip()

def filter_tags(tags, section_id):
    if not isinstance(tags, list):  # Handle None or invalid format
        return pd.Series(["", ""])  # Return empty strings

    # Filter tags matching the given sectionId (ignoring None values)
    filtered = [tag["id"] for tag in tags if str(tag.get("sectionId", "")) == str(section_id) and str(tag.get("id", "")) != f"{section_id}/{section_id}"]
    if not filtered:
        return pd.Series([f"{str(section_id)}/others", f"{str(section_id)}/others"])

    cleaned_filtered = [sub for sub in filtered if sub != "uk/uk"]
    # Create the two columns
    all_titles = ", ".join(cleaned_filtered)
    if cleaned_filtered:
        first_title = cleaned_filtered[0]
    else:
        first_title = f"{str(section_id)}/others"

    return pd.Series([first_title, all_titles])

def pre_processing():
    in_directory = os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_JSON")
    out_directory = os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_CSV")
    for filename in sorted(os.listdir(in_directory)):
        if filename.endswith(".json"):
            f = os.path.join(in_directory, filename)
            output_file = os.path.join(out_directory, f"{os.path.splitext(filename)[0]}.csv")
            df = pd.read_json(f, lines=True)
            data = pd.json_normalize(df["results"][0])
            data["title"] = data["fields.headline"].apply(remove_html_tags)
            data["title"] = data["title"].astype(str).apply(lambda x: x.replace("'", '"'))
            data["abstract"] = data["fields.trailText"].apply(remove_html_tags)
            data["abstract"] = data["abstract"].astype(str).apply(lambda x: x.replace("'", '"'))
            data['text'] = data['title'].astype(str) + " " + data['abstract'].astype(str)
            data["publication_date"] = data["webPublicationDate"]
            data["category"] = data["sectionId"]
            data = data[data['category'].notna() & (data['category'] != '')]
            data[["sub_category", "all_sub_categories"]] =data.apply(lambda row: filter_tags(row['tags'], row['sectionId']), axis=1)
            data = data.drop(["fields.headline", "isHosted", "apiUrl", "tags", "webUrl", "sectionId",
                              "sectionName", "fields.trailText", "pillarId", "pillarName", "webTitle",
                              "id", "type", "webPublicationDate"], axis=1)
            data.to_csv(output_file, sep=";", index=False)

def merge_files():
    input_dir = os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_CSV")
    output_dir = os.getenv("DATASETS_FOLDER_THE_GUARDIAN")
    output_file = os.path.join(output_dir, "the_guardian.csv")
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