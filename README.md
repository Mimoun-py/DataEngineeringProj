## How to run the pipelines batch (/pipeline) and real-time (/pipeline_rt)

### 1. thing you need to do before running the batch processing pipeline

put the yellow_tripdata_2025-01.parquet file inside the /data foler [link](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

- you also need to sort out the .env file where you need the AIRFLOW_UID this you can put to 50000 as it its universly used like that just gives an id to everything that is airflow related.
- then you also need your azure credentioals AZURE_CONNECTION_STRING, AZURE_CONTAINER_NAME if not provided i got you it just writes locally then to the /output folder
- then for the last Eviarment variable you need \_PIP_ADDITIONAL_REQUIREMENTS this is like installing the packeges using pip but airflow does it for you. you can look at the requirements.txt and past it there or even better just copy this `_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow azure-storage-blob python-dotenv numpy`

then if you want to run it you can so manually or by changing the schedule parameter in the [dag](dags\yellow_taxi_dag.py)
then it will run when the cron is set up no need to restart docker as airflow checks dag changes regularly

now it should work and if you set up everything correctly there should be a file locally called yellow_tripdata_processed_2025-01.parquet no matter how many times run youll only see 1 file idempotency its called. and if you have set up your azure credentials right you can check in the logs under dag_id=yellow_taxi_pipeline that it made a folder yellow-taxi-output and inside there is the processed file

### 2. now for the real time set up

not much is needed if you set up your .env file correctly needs to be at root level btw. to use the realtime pipeline
important files for the real time pipeline are the `/input`, `/pipeline_rt`, `/output` and `dags/ecommerce_dag.py`

to activate the pipeline you need to place the ecommerce_orders.csv in the input folder and the pipeline will detect it and process that file if azure credientiels provided it will write to azure if not only locally with the date and time stemp for clerety
