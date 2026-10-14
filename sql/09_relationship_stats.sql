SELECT 
    x_metric, 
    y_metric, 
    method, 
    n_facilities, 
    coefficient, 
    r_squared, 
    slope
FROM gdrive_gold.gold_relationship_stats
ORDER BY y_metric, x_metric, method