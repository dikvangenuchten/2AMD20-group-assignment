from typing import List
import pandas as pd
from rdflib import Graph, URIRef, BNode, Literal, Namespace

def create_graph(df: pd.DataFrame):
    graph = Graph()
    
    namespace = Namespace("http://election/")
    tweets_namespace = Namespace("http://election/tweets/")
    topic_namespace = Namespace("http://election/topics/")
    county_namespace = Namespace("http://election/counties/")

    
    topics = {x: topic_namespace[x] for list_ in df["topics"].values for x in list_}
    for name, topic in topics.items():
        graph.add((topic, namespace.name, Literal(name)))

    counties = {county: county_namespace[county] for county in df["county"].unique()}
    for name, county in counties.items():
        graph.add((county, namespace.name, Literal(name)))

    for _, row in df.iterrows():
        tweet = tweets_namespace[f"{row['tweet_id']}"]
        graph.add((tweet, namespace.sentiment, Literal(row["sentiment"])))
        for topic in row["topics"]:
            graph.add((tweet, namespace.isAbout, topics[topic]))
        graph.add((tweet, namespace.origin, counties[row["county"]]))

    return graph

def get_sentiment_per_topic_for(county_name: str, graph: Graph):
    namespace = Namespace("http://election/")
    
    sentiment_query = f"""
    SELECT ?topic_name (AVG(?sentiment) as ?avg)
    WHERE {{
        ?tweet <{namespace.isAbout}> ?topic .
        ?tweet <{namespace.sentiment}> ?sentiment .
        ?tweet <{namespace.origin}> ?county .
        ?topic <{namespace.name}> ?topic_name .
        ?county <{namespace.name}> "{county_name}" .
    }}
    GROUP BY ?topic_name ?county
    """

    for topic, sentiment in graph.query(sentiment_query):
        print(f"County: {county_name} has a sentiment of {float(sentiment):2.2f} on {topic}")

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
        print(f"County: {county_name} has a sentiment of {float(sentiment):2.2f} on {topic_name}")

def main():
    df = pd.read_csv("example_data.csv", dtype={
        "tweet_id":int,
        "sentiment":int,
        "topics":str,
        "county":str,
        "state":str
        })
    df["topics"] = df["topics"].str.split(";")
    graph = create_graph(df)

    get_sentiment_per_topic_for("Jefferson", graph)
    get_sentiment_per_topic_for("Sparta", graph)
    
    get_sentiment_for_topic_per_county("guns", graph)

if __name__ == "__main__":
    main()