import pandas as pd
import string

TOPICS = [
        "covid19",
        "corona",
        "coronavirus",
        "black lives matter",
        "blm",
        "blacklivesmatter",
        "cnn",
        "foxnews",
        "msnbc",
        "china",
        "tax",
        "taxes",
        "russia",
        "healthcare",
        "obamacare",
        "fraud",
        "vetsforscience",
        "antifa",
    ]

def add_topic(df: pd.DataFrame):
    """Adds a column which contains the preselected topics if they are mentioned in the tweet"""
    topics = [topic.lower() for topic in TOPICS]
    df["topics"] = (
        df["tweet"]
        .str.translate(str.maketrans('', '', string.punctuation)) # Remove all punctuation
        .apply(lambda x: list(set(filter(lambda y: y in topics, x.split(" ")))))
    )
    # Dropping non topics
    return df[df["topics"].map(lambda d: len(d)) > 0]