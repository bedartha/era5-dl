"""
Contains the details of the data request
========================================
"""

COLLECTION_ID = "derived-era5-single-levels-daily-statistics"
REQUEST_ARGS ={
            "product_type": "reanalysis",
            "month": [str(m) for m in range(1, 13)],
            "day": [str(d) for d in range(1, 32)],
            "daily_statistic": "daily_mean",
            "time_zone": "utc+00:00",
            "frequency": "6_hourly",
            "data_format": "netcdf",
        } 
