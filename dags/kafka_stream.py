from airflow.decorators import dag, task
from pendulum import datetime
import uuid

def get_data():
    import requests
    import json
    res = requests.get('https://randomuser.me/api').json()
    return res["results"][0]

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data    

print(format_data(get_data()))

# @dag(
#     dag_id="real_time_streaming",
#     description="This project demonstrate data engineering real time streaming using kafka and spark",
#     schedule="@daily",
#     doc_md=__doc__,
#     default_args={"owner": "Salman Zaidi", "retries": 3},
#     start_date=datetime(2024,12,17),
#     catchup=False,
#     tags=["real time data streaming", "kafka", "spark"],
#     max_consecutive_failed_dag_runs=3
# )
    
# def real_time_data_streaming():
#     @task
#     def stream_data():
#         _stream_data()
#     stream_data()

# real_time_data_streaming()
            
        
        
