from engine import RecommendationEngine

def movie_ratings(user_id, movie_id):
    print("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)

def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)