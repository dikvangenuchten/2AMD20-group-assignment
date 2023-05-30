import geopandas as gpd
import contextily as ctx
from shapely.geometry import Point
import matplotlib.pyplot as plt
import matplotlib.cm as cm


def plot_map(df, color):
    """
    Plots tweets on a map of the USA
    """
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["long"].values, df["lat"].values),
        crs="EPSG:4326",
    )
    df_wm = gdf.to_crs(epsg=3857)
    ax = df_wm.plot(figsize=(10, 10), alpha=0.2, c=cm.coolwarm(color))
    ctx.add_basemap(ax)
    plt.axis("off")
    plt.title("Tweets in USA mentioning Trump or Biden")
    plt.show()
