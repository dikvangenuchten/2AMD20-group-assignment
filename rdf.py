from typing import List
import pandas as pd
from rdflib import Graph, URIRef, BNode, Literal, Namespace


def create_graph(df: pd.DataFrame):
    graph = Graph()

    namespace = Namespace("http://election/")
    tweets_namespace = Namespace("http://election/tweets/")
    topic_namespace = Namespace("http://election/topics/")
    county_namespace = Namespace("http://election/counties/")
    
    # TODO merge some topics
    unique_topics = set()
    for tweet_topics in df["topics"].values:
        for topic in tweet_topics:
            if topic not in unique_topics:
                unique_topics.add(topic)
    
    topics = {x: topic_namespace[x] for x in unique_topics}
    for name, topic in topics.items():
        graph.add((topic, namespace.name, Literal(name)))

    counties = {county: county_namespace[county.lower().replace(" ", "_")] for county in df["county"].unique()}
    for name, county in counties.items():
        graph.add((county, namespace.name, Literal(name)))

    for _, row in df.iterrows():
        tweet = tweets_namespace[f"{row['tweet_id']}"]
        graph.add((tweet, namespace.sentiment, Literal(row["compound"]["compound"])))
        for topic in row["topics"]:
            graph.add((tweet, namespace.isAbout, topics[topic]))
        graph.add((tweet, namespace.origin, counties[row["county"]]))
        graph.add((tweet, namespace.retweets, Literal(row["retweet_count"])))
        graph.add((tweet, namespace.likes, Literal(row["likes"])))

    return graph


def get_sentiment_per_topic_for(county_name: str, graph: Graph):
    namespace = Namespace("http://election/")

    sentiment_query = f"""
    SELECT ?topic_name (AVG(?sentiment) as ?avg) (COUNT(?tweet) as ?count)
    WHERE {{
        ?tweet <{namespace.isAbout}> ?topic .
        ?tweet <{namespace.sentiment}> ?sentiment .
        ?tweet <{namespace.origin}> ?county .
        ?topic <{namespace.name}> ?topic_name .
        ?county <{namespace.name}> "{county_name}" .
    }}
    GROUP BY ?topic_name ?county
    """
    
    topics = []
    sentiments = []
    counts = []
    for topic, sentiment, count in graph.query(sentiment_query):
        topics.append(str(topic))
        sentiments.append(float(sentiment))
        counts.append(int(count))

    return topics, sentiments, counts


def get_sentiment_for_topic_per_county(topic_name: str, graph: Graph):
    namespace = Namespace("http://election/")

    sentiment_query = f"""
    SELECT ?county_name (AVG(?sentiment) as ?avg)
    WHERE {{
        ?tweet <{namespace.isAbout}> ?topic .
        ?tweet <{namespace.sentiment}> ?sentiment .
        ?tweet <{namespace.origin}> ?county .
        ?county <{namespace.name}> ?county_name .
        ?topic <{namespace.name}> "{topic_name}" .
    }}
    GROUP BY ?county_name ?topic
    """

    for county_name, sentiment in graph.query(sentiment_query):
        print(
            f"County: {county_name} has a sentiment of {float(sentiment):2.2f} on {topic_name}"
        )


def get_sentiment_per_topic_for_distributed(county_name: str, graph: Graph):
    namespace = Namespace("http://election/")

    sentiment_query = f"""
    SELECT ?topic_name ?sentiment
    WHERE {{
        ?tweet <{namespace.isAbout}> ?topic .
        ?tweet <{namespace.sentiment}> ?sentiment .
        ?tweet <{namespace.origin}> ?county .
        ?topic <{namespace.name}> ?topic_name .
        ?county <{namespace.name}> "{county_name}" .
    }}
    
    """

    topics = []
    sentiments = []
    counts = []
    for topic, sentiment in graph.query(sentiment_query):
        topics.append(str(topic))
        sentiments.append(float(sentiment))

    return topics, sentiments

def main():
    df = pd.read_csv(
        "example_data.csv",
        dtype={
            "tweet_id": int,
            "sentiment": int,
            "topics": str,
            "county": str,
            "state": str,
        },
    )
    df["topics"] = df["topics"].str.split(";")
    graph = create_graph(df)

    get_sentiment_per_topic_for("Jefferson", graph)
    get_sentiment_per_topic_for("Sparta", graph)

    get_sentiment_for_topic_per_county("guns", graph)


if __name__ == "__main__":
    main()
