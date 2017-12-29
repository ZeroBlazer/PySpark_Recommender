import time, sys, cherrypy, os
from paste.translogger import TransLogger
from pyspark import SparkContext, SparkConf
from engine import RecommendationEngine

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
 
    return sc

def movie_ratings(user_id, movie_id):
    print("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)

def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    dataset_path = os.path.join('data', 'ml-latest-small')
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)
