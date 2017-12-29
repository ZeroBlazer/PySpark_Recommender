import time, sys, cherrypy, os
from pyspark import SparkContext, SparkConf
from engine import RecommendationEngine

import json
import time

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    # sc = SparkContext(conf=conf, pyFiles=['engine.py'])
 
    return sc

def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)

def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)

def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    dataset_path = os.path.join('..', 'data', 'ml-latest-small')
    app = create_app(sc, dataset_path)

    print("RECOMMENDER STARTED!!! >>>")

    start_time = time.time()

    for i in range(1,10):
        # logger.debug("Recommendation for %s |>", i, top_ratings(i, 1))
        print("Recommendation for", i, " |> ", top_ratings(i, 1))

    print("Calculated in %s seconds" % (time.time() - start_time))