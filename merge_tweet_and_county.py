import pandas as pd
from functools import partial
from geopy.distance import distance as geo_distance
from tqdm.contrib.concurrent import process_map


def add_closest_county(tweet, all_counties):
    print(tweet)
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


if __name__ == "__main__":
    df_counties = pd.read_csv("datasets/uscounties/uscounties.csv")
    df_tweets_donald = pd.read_csv(
        "datasets/tweets/hashtag_donaldtrump.csv",
        lineterminator="\n",
    )

    df_tweets_joe = pd.read_csv(
        "datasets/tweets/hashtag_joebiden.csv",
        lineterminator="\n",
    )

    county_dict = {
        x.county: (x.lat, x.lng)
        for x in df_counties[["county", "lat", "lng"]].itertuples()
    }
    
    print("starting")
    result = process_map(
        lambda row: add_closest_county(row[1], county_dict), df_tweets_donald.iterrows()
    )
    print(result[0])

    df_tweets_donald["county"] = df_tweets_donald.progress_apply(
        partial(add_closest_county, all_counties=county_dict), axis=1
    )
    df_tweets_donald.to_csv(
        "datasets/tweets/hashtag_donaldtrump_with_county.csv",
        lineterminator="\n",
    )

    df_tweets_joe["county"] = df_tweets_joe.apply(
        partial(add_closest_county, all_counties=county_dict), axis=1
    )
    df_tweets_joe.to_csv(
        "datasets/tweets/hashtag_joebiden_with_county.csv",
        lineterminator="\n",
    )
