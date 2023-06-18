from typing import Union
import pandas as pd

from langdetect import detect

from tqdm import tqdm

from functools import partial

import nltk

nltk.download("vader_lexicon")
from nltk.sentiment.vader import SentimentIntensityAnalyzer

tqdm.pandas()


def language(x):
    try:
        return detect(str(x))
    except:
        return None


def vader_sentiment(tweet, sid):
    try:
        return sid.polarity_scores(tweet)
    except:
        return None


def output(vader_dict):
    # Nonetype threw an error (while I filter those out later) so I use another try except here
    try:
        polarity = "neutral"
        if vader_dict["compound"] >= 0.05:
            polarity = "positive"
        elif vader_dict["compound"] <= -0.05:
            polarity = "negative"

        return polarity
    except:
        return None

def load_data(name: str = "joebiden"):
    return pd.read_csv(f"datasets/tweets/hashtag_{name}.csv", lineterminator="\n")


def add_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    sid = SentimentIntensityAnalyzer()
    df["language"] = df["tweet"].progress_apply(language)
    # df.to_csv(f"{name}_tweet_with_language.csv")

    # Filter on only English for sentiment analysis, use original dataset for further visualisation
    df_en = df[df["language"] == "en"]
    print(len(df_en))  # Keep track how many were filtered out

    df_en["compound"] = df_en["tweet"].progress_apply(partial(vader_sentiment, sid=sid))
    # Filter out Nonevalues since not needed for analysis
    df_en = df_en[df_en["compound"] != None]
    # Keep track how many were filtered out
    print(len(df_en))

    df_en["emotion"] = df_en["compound"].progress_apply(output)
    return df_en

def main(name: Union[str, pd.DataFrame] = "joebiden"):
    df = load_data(name)
    df_en = add_sentiment(df)
    df_en.to_csv(f"{name}_english_tweets_with_compound.csv")
    return df_en


if __name__ == "__main__":
    main("joebiden")
    main("donaldtrump")
