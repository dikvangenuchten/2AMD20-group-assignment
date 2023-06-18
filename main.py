import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import merge_tweet_and_county
import sentiment_analysis
import topic_extraction
import rdf

def load_data(name: str = "joebiden"):
    return pd.read_csv(f"datasets/tweets/hashtag_{name}.csv", lineterminator="\n")


def preprocess_df(name: str, df_counties):
    print(f"Loading {name}")
    df_pres = load_data(name)
    print(f"{name} contains {len(df_pres)} tweets")
    
    print(f"Extracting topics from {name}")
    df_pres = topic_extraction.add_topic(df_pres)
    print(f"{name} contains {len(df_pres)} tweets with relevant topics")
    
    print(f"Adding county information to {name}")
    df_pres = merge_tweet_and_county.main(df_pres, df_counties, name)
    print(f"{name} contains {len(df_pres)} from relevant counties")
    
    print(f"Adding sentiment analysis to {name}")
    df_pres = sentiment_analysis.add_sentiment(df_pres)
    print(f"{name} contains {len(df_pres)} with english tweets")
    
    return df_pres
 
def load_cache_counties() -> pd.DataFrame:
    cache_path = os.path.join("datasets", "cache", f"counties.pkl")
    if not os.path.exists(cache_path):
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df_counties = merge_tweet_and_county.load_df_counties()
        df_counties.to_pickle(cache_path)
    else:
        print(f"Loading counties from cache. ({cache_path})")
    return pd.read_pickle(cache_path)

def load_cache_pres(name: str, counties: pd.DataFrame) -> pd.DataFrame:
    cache_path = os.path.join("datasets", "cache", f"{name}.pkl")
    if not os.path.exists(cache_path):
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df_pres = preprocess_df(name, counties)
        df_pres.to_pickle(cache_path)
    else:
        print(f"Loading {name} from cache. ({cache_path})")
    return pd.read_pickle(cache_path)

def create_graph():
    print("Loading counties")
    df_counties = load_cache_counties()
    
    df_trump = load_cache_pres("donaldtrump", df_counties)
    df_biden = load_cache_pres("joebiden", df_counties)

    df_all = pd.concat([df_biden, df_trump]).drop_duplicates(subset="tweet_id")
    print(f"Finally having {len(df_all)} tweets left")
    graph = rdf.create_graph(df_all)
    return graph

def make_bubble_graph(topic, sentiment, size_data, name: str):
    fig = plt.figure(figsize=(10, 10))
    
    plt.scatter(
        x=sentiment,
        y=topic,
        s=size_data,
        c=sentiment,
        linewidths=2
    )
    
    plt.xlim(-1, 1)
    plt.xlabel("Sentiment")
    plt.ylabel("Topic")
    plt.title(f"Sentiment per topic for {name}")
    plt.legend()
    plt.colorbar()
    fig.savefig(f"bubble_{name}.png")

def main():
    graph = create_graph()

    # What is the sentiment of Hudson County
    county = "Hudson County"
    topic, sentiment, count = rdf.get_sentiment_per_topic_for(county, graph)

    make_bubble_graph(
        topic,
        sentiment,
        np.asarray(count) ** 1.2,
        county
    )
    pass
   

if __name__ == "__main__":
    main()