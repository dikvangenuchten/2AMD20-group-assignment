import pandas as pd

# import polars as pd
import collections


def main(df: pd.DataFrame):
    df["hashtags"] = df["tweet"].str.lower().str.findall(r"#(\w+)")
    # df = df.with_columns(hashtags=df["tweet"].str.to_lowercase().str.extract_all(r"#(\w+)"))
    count_hashtags = collections.Counter(
        element for list_ in df["hashtags"].values for element in list_
    ).most_common()
    print(count_hashtags)
    pass


def split_dataset_by_mention(df_trump: pd.DataFrame, df_biden: pd.DataFrame):
    df = pd.concat([df_trump, df_biden])

    df_trump = df[
        (df["tweet"].str.contains("Trump") & ~(df["tweet"].str.contains("Biden")))
    ]
    df_biden = df[
        (df["tweet"].str.contains("Biden") & ~(df["tweet"].str.contains("Trump")))
    ]
    df_both = df[df["tweet"].str.contains("Biden") & df["tweet"].str.contains("Trump")]

    return df_trump, df_biden, df_both


if __name__ == "__main__":
    df_trump = pd.read_csv(
        f"datasets/tweets/hashtag_donaldtrump.csv", lineterminator="\n"
    )
    df_biden = pd.read_csv(f"datasets/tweets/hashtag_joebiden.csv", lineterminator="\n")
    main(df_trump)

    df_onlytrump, df_onlybiden, df_both = split_dataset_by_mention(df_trump, df_biden)
