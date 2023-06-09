import numpy as np
import pandas as pd
from functools import partial

import tqdm
from tqdm.contrib.concurrent import process_map


def find_closest_county(row, county_points: np.ndarray, county_names: pd.DataFrame):
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
    county = county_names.reset_index().iloc[closest_idx]

    if dist > 1000:
        # Most likely not an american tweeting
        # print(f"\n{tweet}\n")
        return {k: None for k in county.to_dict()}

    return county.to_dict()


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


def main(df_tweets: pd.DataFrame, counties: pd.DataFrame, name: str):
    # Filter out only america
    print(f"Loaded {len(df_tweets)} tweets.")
    df_tweets = df_tweets[df_tweets["country"] == "United States of America"]
    print(f"{len(df_tweets)} are from US teritory.")
    # county_names = list(counties["county"])

    county_points = np.asarray(counties[["lat", "lng"]])

    county = [
        find_closest_county(
            row,
            county_points=county_points,
            county_names=counties,
        )
        for row in tqdm.tqdm(
            df_tweets.iterrows(),
            total=df_tweets.shape[0],
            desc="Adding county to tweets",
        )
    ]

    # Conflict between state from location and state from twitter
    # After looking at some of the conflicts, these are all close to state borders.
    # Twitter data has more empty values on state, therefor we chose to use our
    # calculated state instead of the one from twitter
    df_county = pd.DataFrame.from_records(county, index=df_tweets.index).drop(
        ["lat", "lng"], axis=1
    )
    df_tweets = df_tweets.drop("state", axis=1)
    df_tweets = df_tweets.join(df_county)

    # Remove tweets that do not have a county
    # (E.g. puerto rico, guam) as they have no vote during general election
    df_tweets = df_tweets[df_tweets["county"].notnull()]
    print(f"{len(df_tweets)} are from valid counties.")

    df_tweets = classify_user_voting_preference_based_on_county(
        df_tweet_county=df_tweets
    )

    df_tweets.to_csv(
        f"datasets/tweets/hashtag_{name}_with_county.csv",
        lineterminator="\n",
    )
    print(f"Saved data: {name}")
    return df_tweets


def apply_func(x):
    return pd.Series(
        {
            # "state": x["state"].values[0],
            # "county": x["county"].values[0],
            "total_votes": x["total_votes"].sum(),
            "donald_trump_votes": x[x["candidate"] == "Donald Trump"][
                "total_votes"
            ].values[0],
            "joe_biden_votes": x[x["candidate"] == "Joe Biden"]["total_votes"].values[
                0
            ],
        }
    )


def add_election_result_to_county(
    df_counties: pd.DataFrame, df_election: pd.DataFrame
) -> pd.DataFrame:
    df_counties_multi = df_counties.rename(
        columns={"state_name": "state", "county_full": "county"}
    ).set_index(["state", "county"])

    df_election = df_election.set_index(["state", "county"])
    df_election_per_county = df_election.groupby(["state", "county"]).apply(apply_func)
    df_election_per_state = df_election.groupby(["state"]).apply(apply_func)

    df_counties_with_votes = df_counties_multi.join(df_election_per_county)
    invalid = len(df_counties_with_votes[df_counties_with_votes["total_votes"].isna()])
    print(
        f"There are {invalid} counties that could not be matched. Using state votes for those."
    )

    df_counties_with_votes["votes_are_from_county"] = True
    df_counties_with_votes[df_counties_with_votes["total_votes"].isna()][
        "votes_are_from_county"
    ] = False
    df_counties_with_votes = df_counties_with_votes.join(
        df_election_per_state, lsuffix="_county", rsuffix="_state"
    )

    return df_counties_with_votes


def classify_user_voting_preference_based_on_county(
    df_tweet_county: pd.DataFrame,
) -> pd.DataFrame:
    # TODO double check ordering
    percentage_biden =  df_tweet_county["joe_biden_votes_county"] / df_tweet_county["total_votes_county"]
    df_tweet_county["voter_preference"] = ""
    df_tweet_county["voter_preference"][percentage_biden.between(0.00, 0.20, inclusive="left")] = "Full Republican"
    df_tweet_county["voter_preference"][percentage_biden.between(0.20, 0.45, inclusive="left")] = "Slightly Republican"
    df_tweet_county["voter_preference"][percentage_biden.between(0.45, 0.55, inclusive="both")] = "Swing State"
    df_tweet_county["voter_preference"][percentage_biden.between(0.55, 0.80, inclusive="right")] = "Slightly Democratic"
    df_tweet_county["voter_preference"][percentage_biden.between(0.80, 1.00, inclusive="right")] = "Full Democratic"

    return df_tweet_county

def load_df_counties():
    print("Loading county dataset")
    df_counties = pd.read_csv("datasets/uscounties/uscounties.csv")[
        ["state_name", "county_full", "lat", "lng"]
    ]

    print("Loading election dataset")
    df_election = pd.read_csv(
        "datasets/election_results/president_county_candidate.csv"
    )

    return add_election_result_to_county(df_counties, df_election)

def load_data(name: str = "joebiden"):
    return pd.read_csv(f"datasets/tweets/hashtag_{name}.csv")

if __name__ == "__main__":
    df_counties = load_df_counties()
    df_trump = load_data("donaldtrump")
    main(df_trump, df_counties, "donaldtrump")
    df_biden = load_data("donaldtrump")
    main(df_biden, df_counties, "joebiden")
