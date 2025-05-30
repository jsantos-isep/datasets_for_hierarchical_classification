import os

import pandas as pd
from dotenv import load_dotenv
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import json
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split


load_dotenv()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    return parser

def split_at_first_space(s):
    parts = s.split(' ', 1)
    if len(parts) > 1:
        return parts[0] + '\n' + parts[1]
    else:
        return s

if __name__ == "__main__":
    args = get_args().parse_args()
    input_dir = os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")
    file = os.path.join(input_dir, f"{str(args.dataset)}.csv")
    df = pd.read_csv(file, sep=";")

    if args.dataset == "the_guardian":
        # Count occurrences of each category
        df = df[~df['category'].isin(["theguardian", "news", "theobserver", "commentisfree"])]
        category_counts = df['category'].value_counts()
        df = df[df['category'].isin(category_counts[category_counts > 2500].index)]
        filtered_categories = category_counts[category_counts > 2500]
        top_categories = filtered_categories.head(5)
    else:
        category_counts = df['category'].value_counts()
        df = df[df['category'].isin(category_counts[category_counts > 10].index)]
        filtered_categories = category_counts[category_counts > 10]
        top_categories = filtered_categories.head(5)

    # Bar plot for categories
    # plt.figure(figsize=(30, 4))
    # sns.countplot(x=df['category'], order=df['category'].value_counts().index, palette="viridis")
    # plt.title("Category Distribution")
    # plt.xlabel("Category")
    # plt.xticks(rotation=90)
    # plt.ylabel("Count")
    plt.figure(figsize=(6, 5))
    split_labels = [cat.replace('_', '\n') for cat in top_categories.index]

    plt.bar(split_labels, top_categories.values, color='grey')
    #plt.title(f'The Guardian - Top 5 Categories Distribution')
    plt.xlabel('Category')
    plt.ylabel('Count')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()
    plt.savefig(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_categories.png", dpi=500, bbox_inches="tight")

    if args.dataset == "the_guardian":
        # Count occurences of each subcategory
        subcategory_counts = df['sub_category'].value_counts()

        df = df[df['sub_category'].isin(subcategory_counts[subcategory_counts > 1000].index)]
    else:
        subcategory_counts = df['sub_category'].value_counts()

        df = df[df['sub_category'].isin(subcategory_counts[subcategory_counts > 10].index)]

    # Remove duplicate category-subcategory pairs
    df_unique = df.drop_duplicates(subset=['category', 'sub_category'])

    # Group subcategories under their respective categories
    grouped = df_unique.groupby('category')['sub_category'].apply(list).to_dict()

    # Export to JSON file
    with open(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/ontology_all.json", 'w') as f:
        json.dump(grouped, f, indent=4)

    df = df.sort_values(by='publication_date')

    # Calculate split index (10% for training)
    train_df, temp_df = train_test_split(df, test_size=0.2, stratify=df["category"], random_state=0)
    train_df = train_df.sort_values(by='publication_date')

    val_df, test_df = train_test_split(temp_df, test_size=0.5, stratify=temp_df["category"], random_state=0)
    val_df = val_df.sort_values(by='publication_date')
    test_df = test_df.sort_values(by='publication_date')

    #train_df.to_csv(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_train.csv", sep=";", index=False)

    train_grouped_dfs = {key: group for key, group in train_df.groupby('category')}

    for key, group in train_grouped_dfs.items():
        filename = f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_train_{key.lower()}.csv"  # You can customize the filename pattern
        group.to_csv(filename, sep=";", index=False)  # index=False to avoid writing row numbers

    # Remove duplicate category-subcategory pairs
    df_unique_train = train_df.drop_duplicates(subset=['category', 'sub_category'])

    # Group subcategories under their respective categories
    grouped_train = df_unique_train.groupby('category')['sub_category'].apply(list).to_dict()

    # Export to JSON file
    with open(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/initial_ontology.json", 'w') as f:
        json.dump(grouped_train, f, indent=4)

    #test_df.to_csv(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_test.csv", sep=";", index=False)

    #val_df.to_csv(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_val.csv", sep=";", index=False)

    val_grouped_dfs = {key: group for key, group in val_df.groupby('category')}

    for key, group in val_grouped_dfs.items():
        filename = f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/{str(args.dataset)}_val_{key.lower()}.csv"  # You can customize the filename pattern
        group.to_csv(filename, sep=";", index=False)  # index=False to avoid writing row numbers

    title_average_length = df['title'].str.len().mean()
    abstract_average_length = df['abstract'].str.len().mean()

    summary_df = pd.DataFrame({
        'Metric': ['Number of Documents', 'Number of Categories', 'Number of Subcategories', 'Number of training docs', 'Number of validation docs', 'Number of testing docs', 'Average length of title', 'Average length of abstract'],
        'Value': [len(df), df['category'].nunique(), df['sub_category'].nunique(), len(train_df), len(val_df), len(test_df), int(title_average_length), int(abstract_average_length)]
    })

    summary_df.to_csv(f"{os.getenv(f"DATASETS_FOLDER_{str(args.dataset).upper()}")}/summary.csv", index=False)



