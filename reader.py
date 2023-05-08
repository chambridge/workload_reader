import os
import boto3
import csv
import json

s3 = boto3.resource('s3')

bucket_name = os.getenv("S3_BUCKET")
bucket = s3.Bucket(bucket_name)

customers = []
cust_dict = {}

for bucket_object in bucket.objects.all():
    split_key = bucket_object.key.split("/")
    customer = split_key[0]
    cluster_id = split_key[1]
    data_date = split_key[2]
    file = split_key[3]
    date_dict = {"date": data_date, "files":[file], "keys":[bucket_object.key]}
    cluster_dict = {"cluster": cluster_id, "dates":{data_date: date_dict}}

    cur_cust_dict = cust_dict.get(customer)
    if not cur_cust_dict:
        cust_dict[customer] = {"customer": customer, "clusters":{cluster_id: cluster_dict}}
    else:
        clusters = cur_cust_dict.get("clusters")
        cur_cluster_dict = clusters.get(cluster_id)
        if not cur_cluster_dict:
            clusters[cluster_id] = cluster_dict
        else:
            dates_dict = cur_cluster_dict.get("dates")
            cur_date_dict = dates_dict.get(data_date)
            if not cur_date_dict:
                dates_dict[data_date] = date_dict
            else:
                files = cur_date_dict.get("files")
                files.append(file)
                keys = cur_date_dict.get("keys")
                keys.append(bucket_object.key)


    customers.append(customer)

customers=list(set(customers))
# print(f"customers={len(customers)}")

allow_list = os.getenv("ALLOW_LIST", "").split(",")
cust_map_str = os.getenv("CUSTOMER_MAP", "[]")
cust_map = json.loads(cust_map_str)

acct_map = {}
org_map = {}
for cust in cust_map:
    acct = cust.get("account")
    org = cust.get("org")
    acct_map[acct] = org
    org_map[org] = acct

for cust in allow_list:
    total_workloads = []
    cur_acct = None
    cur_org = None
    if "acct" in cust:
        cur_acct = cust[4:]
        cur_org = acct_map[cur_acct]
    if "org" in cust:
        cur_org = cust[3:]
        cur_acct = org_map[cur_org]

    cur_cust_dict = cust_dict.get(cust)
    for cluster_key, cluster in cur_cust_dict.get("clusters").items():
        cur_dates = cluster.get("dates")
        sorted_dates = list(cur_dates.keys())
        sorted_dates.sort(reverse=True)
        selected_date = sorted_dates[0]
        cur_date_dict = cur_dates.get(selected_date)
        cur_keys = cur_date_dict.get("keys")
        workloads = []
        for cur_key in cur_keys:
            os.makedirs(os.path.dirname(cur_key), exist_ok=True)
            bucket.download_file(cur_key, cur_key)
            with open(cur_key, mode='r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                line_count = 0
                for row in csv_reader:
                    if line_count != 0:
                        workload = row.get("workload")
                        if workload not in workloads:
                            workloads.append(workload)
                    line_count += 1
        cluster["workloads"] = workloads
        total_workloads += workloads
        print(f"account: {cur_acct}, org: {cur_org} - cluster:{cluster_key}, workload count:{len(workloads)}")
    print(f"account: {cur_acct}, org: {cur_org} , total workload count: {len(total_workloads)}")
    print()