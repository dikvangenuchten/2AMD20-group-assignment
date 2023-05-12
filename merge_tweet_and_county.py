import pandas as pd
from functools import partial
from geopy.distance import distance as geo_distance
from tqdm.contrib.concurrent import process_map


def add_closest_county(row, all_counties):
    tweet = row[1]
    if (
        tweet["lat"] is None
        or tweet["long"] is None
        or pd.isna(tweet["lat"])
        or pd.isna(tweet["long"])
    ):
        return None

    lat, lon = tweet["lat"], tweet["long"]
    dist, county = min(
        (geo_distance((lat, lon), coord), county)
        for county, coord in all_counties.items()
    )

    if dist > 100:
        print(dist, tweet["country"], tweet["city"])

    return county

def main(name: str, counties: dict):
    print(f"Starting: {name}")
    df_tweets = pd.read_csv(
        f"datasets/tweets/hashtag_{name}.csv",
        lineterminator="\n",
    )
    print("Loaded data")
    result = process_map(
        partial(add_closest_county, all_counties=counties), df_tweets.iterrows()
    )
    print("Processed data")
    df_tweets["county"] = pd.Series(result)
    df_tweets.to_csv(
        f"datasets/tweets/hashtag_{name}_with_county.csv",
        lineterminator="\n",
    )
    print(f"Saved data: {name}")

if __name__ == "__main__":
    df_counties = pd.read_csv("datasets/uscounties/uscounties.csv")
    county_dict = {
        x.county: (x.lat, x.lng)
        for x in df_counties[["county", "lat", "lng"]].itertuples()
    }
    main("donaldtrump", county_dict)
    main("joebiden", county_dict)
