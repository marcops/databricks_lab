from dataclasses import dataclass
import requests
import time

@dataclass(init=True)
class DatabricksJob:
  # Set up the API endpoint and token
  databricks_url: str
  api_token: str 
  api_endpoint: str = f"{databricks_url}/api/2.0/jobs"
 
  def run_job_from_id(self, job_id):
    payload = {
      "job_id": job_id
    }
    # Call the job in parallel
    post_response = requests.post(f"{api_endpoint}/run-now", headers={"Authorization": f"Bearer {api_token}"}, json=payload)
    #print(post_response.content)
    return post_response.json()["run_id"]

  def discover_job_id(self, job_name):
    get_response = requests.get(f"{api_endpoint}/list", headers={"Authorization": f"Bearer {api_token}"})
    for item in get_response.json()["jobs"]:
      if item["settings"]["name"].upper() == job_name.upper():
        return item["job_id"]
    return None

  def run_job_from_name(self, job_name):
    job_id = self.discover_job_id(job_name)
    print(f"job_id={job_id}")
    run_id = self.run_job_from_id(job_id)
    print(f"run_id={run_id}")
    return run_id

  def wait_finish(self, run_id):
    # Wait finish
    while True:
        get_response = requests.get(f"{api_endpoint}/runs/get?run_id={run_id}", headers={"Authorization": f"Bearer {api_token}"})
        #print(get_response.content)
        status = get_response.json()["state"]["life_cycle_state"]
        print(f"status={status}")
        if status in ("TERMINATED", "INTERNAL_ERROR"):
            return ""
        time.sleep(5)



# And that's all you need to run:
databricks_url = "https://xxx.azuredatabricks.net"
api_token="xxx"
job_name = "job_name"

dj = DatabricksJob(databricks_url, api_token)
run_id = dj.run_job_from_name(job_name)
dj.wait_finish(run_id)
