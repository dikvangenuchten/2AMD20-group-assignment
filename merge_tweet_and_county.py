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


if __name__ == "__main__":
    df_counties = pd.read_csv("datasets/uscounties/uscounties.csv")
    df_tweets_donald = pd.read_csv(
        "datasets/tweets/hashtag_donaldtrump.csv",
        lineterminator="\n",
    ).head(100)

    county_dict = {
        x.county: (x.lat, x.lng)
        for x in df_counties[["county", "lat", "lng"]].itertuples()
    }
    
    print("starting trump_tweets")
    result = process_map(
        partial(add_closest_county, all_counties=county_dict), df_tweets_donald.iterrows()
    )
    print(result[0])
    df_tweets_donald["county"] = pd.Series(result)

    df_tweets_donald.to_csv(
        "datasets/tweets/hashtag_donaldtrump_with_county.csv",
        lineterminator="\n",
    )
    
    df_tweets_joe = pd.read_csv(
        "datasets/tweets/hashtag_joebiden.csv",
        lineterminator="\n",
    ).head(100)


    df_tweets_joe["county"] = df_tweets_joe.apply(
        partial(add_closest_county, all_counties=county_dict), axis=1
    )
    df_tweets_joe.to_csv(
        "datasets/tweets/hashtag_joebiden_with_county.csv",
        lineterminator="\n",
    )
