import pandas as pd
# import polars as pd
import collections

def main(name: str):
    df = pd.read_csv(f"datasets/tweets/hashtag_{name}.csv", lineterminator="\n")
    df["hashtags"] = df["tweet"].str.lower().str.findall(r"#(\w+)")
    # df = df.with_columns(hashtags=df["tweet"].str.to_lowercase().str.extract_all(r"#(\w+)"))
    count_hashtags = collections.Counter(element for list_ in df["hashtags"].values for element in list_).most_common()
    print(count_hashtags)
    pass

if __name__ == "__main__":
    main("donaldtrump")