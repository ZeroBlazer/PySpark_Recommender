source ~/storage/Apps/script/distributed.sh
# spark-submit --master spark://169.254.206.2:7077 --total-executor-cores 14 --executor-memory 6g src/main.py 
spark-submit --master local main.py 

# >>> variables= {}
# >>> execfile("main.py", variables)