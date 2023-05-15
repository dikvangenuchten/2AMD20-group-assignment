import numpy as np
import pandas as pd
from functools import partial

import tqdm
from tqdm.contrib.concurrent import process_map


def find_closest_county(row, county_points: np.ndarray, county_names: list):
    tweet = row[1]
    if (
        tweet["lat"] is None
        or tweet["long"] is None
        or pd.isna(tweet["lat"])
        or pd.isna(tweet["long"])
    ):
        # No location information
        return None

    lat, lon = tweet["lat"], tweet["long"]

    distances = calculate_distance_vectorized((lat, lon), county_points)
    closest_idx = np.argmin(distances)
    dist = distances[closest_idx]
    county = county_names[closest_idx]

    if dist > 100:
        # Most likely not an american tweeting
        return None

    return county


def calculate_distance_vectorized(point: np.ndarray, county_points: np.ndarray) -> int:
    """Calculates the distance on a sphere in kilometers.

    Using the [haversine formula](https://en.wikipedia.org/wiki/Haversine_formula).
    This is an approximation as the world is not a perfect sphere
    """
    radius = 6371  # kilometers

    lat1 = point[0]
    lon1 = point[1]
    lat2 = county_points[:, 0]
    lon2 = county_points[:, 1]

    phi1 = lat1 * np.pi / 180
    phi2 = lat2 * np.pi / 180
    delta_phi = (lat2 - lat1) * np.pi / 180

    delta_lmd = (lon2 - lon1) * np.pi / 180
    a = np.sin(delta_phi / 2) * np.sin(delta_phi / 2) + np.cos(phi1) * np.cos(
        phi2
    ) * np.sin(delta_lmd / 2) * np.sin(delta_lmd / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return radius * c


def main(name: str, counties: pd.DataFrame):
    print(f"Starting: {name}")
    df_tweets = pd.read_csv(
        f"datasets/tweets/hashtag_{name}.csv",
        lineterminator="\n",
    )
    county_names = list(counties["county"])
    county_points = np.asarray(counties[["lat", "lng"]])
    print("Loaded data")

    result = [
        find_closest_county(row, county_points, county_names)
        for row in tqdm.tqdm(
            df_tweets[["lat", "long"]].iterrows(), total=df_tweets.shape[0]
        )
    ]

    print("Processed data")
    df_tweets["county"] = pd.Series(result)
    df_tweets.to_csv(
        f"datasets/tweets/hashtag_{name}_with_county.csv",
        lineterminator="\n",
    )
    print(f"Saved data: {name}")


if __name__ == "__main__":
    df_counties = pd.read_csv("datasets/uscounties/uscounties.csv")[
        ["county", "lat", "lng"]
    ]

    main("donaldtrump", df_counties)
    main("joebiden", df_counties)
