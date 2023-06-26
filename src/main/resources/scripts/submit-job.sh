spark-submit 
  --class org.mausam.poc.aggregator.MetricsAggregator \
  --master yarn \
  --deploy-mode cluster \
  /apps/jar/metrics-aggregator-1.0.0.jar \
  Metrics_Aggregator /Users/mausam/metrics.csv | 4 /Users/mausam/aggregated_metrics