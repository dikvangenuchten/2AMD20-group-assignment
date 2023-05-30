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


def add_president_mention(df: pd.DataFrame):
    df["mentions_only_trump"] = df["tweet"].str.contains("Trump") & ~(
        df["tweet"].str.contains("Biden")
    )
    df["mentions_only_biden"] = df["tweet"].str.contains("Biden") & ~(
        df["tweet"].str.contains("Trump")
    )
    df["mentions_both"] = df["tweet"].str.contains("Biden") & df["tweet"].str.contains(
        "Trump"
    )

    return df


if __name__ == "__main__":
    df_trump = pd.read_csv(
        f"datasets/tweets/hashtag_donaldtrump.csv", lineterminator="\n"
    )
    df_biden = pd.read_csv(f"datasets/tweets/hashtag_joebiden.csv", lineterminator="\n")
    # main(df_trump)

    df = pd.concat([df_trump, df_biden])
    df = add_president_mention(df)

)
