"""
This locust test randomly gets CIDs from a
storetheindex daemon. CIDs are chosen randomly following a 
ZIPF distribution. 
"""

import numpy as np

import time
from locust import HttpUser, task, between
import random

# Initializing Zipf distribution
a = 2.  # Zipf parameter (feel free to change it)

# Read from a list of imported cids
def read_cids(filepath):
    with open (filepath) as file1:
        lines = file1.readlines()
        return [line.strip() for line in lines]

# Choosing a random CID from the list of imported ones
def random_sample(cids, is_zipf=True):
    if is_zipf:
        return cids[np.random.zipf(a)%len(cids)]

    return random.choice(cids)


# Importing CIDs to request
cids = read_cids("/tmp/cids.data")
print("Reading CIDs complete!")

# Test case
class IndexUser(HttpUser):
    #  wait_time = between(1, 2.5)

    @task
    def get_cid(self):
        # Get random cid Zipf distribution
        self.client.get("/cid/"+random_sample(cids))

