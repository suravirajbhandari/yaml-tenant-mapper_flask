import mysql.connector
import pandas as pd
from collections import defaultdict
import trino

def query_from_db(db, query, dataframe:bool=True):
    if db.lower() == 'trino':
        conn=trino.dbapi.connect(host='presto.bstis.com',port=8080,user='hadoop',catalog='hive', schema='prod')
    if db.lower() == 'mysql':
        conn=mysql.connector.connect(user='frontoffice', password='bis12345', host='frontoffice-aws-dev.cav6rhg74o38.us-west-2.rds.amazonaws.com',database='hyperloop')
    cur = conn.cursor()
    cur.execute(query)
    try:
        res = cur.fetchall()
        if dataframe:
            col_names = list(i[0] for i in cur.description)
            resultDf = pd.DataFrame(data=res, columns = col_names)
            return resultDf
        else:
            return res
    except Exception as e:
        return e

def get_tenants():
    query = "select distinct hl_site from hyperloop.job_log where hl_user != 'pentaho' and hl_table like 'asl_calcs,%' and hl_group='' and hl_status='Done' and hl_site !=''"
    tenants = query_from_db('mysql', query, False)
    tenantCheckList = [tenant[0] for tenant in tenants]
    return tenantCheckList

def get_paras(feed, result_list=[]):
    tenants = get_tenants()
    for tenant_id, tenant in enumerate(tenants):
        if feed.lower() == 'elig':
            query = "select tenantid, tpa, params from {}.etl_log2 where status ='finished' and task_name='full_elig_incr' and params like 'process_type=elig%' order by process_time desc limit 1".format(tenant)
        elif feed.lower() == 'med':
            query = "select tenantid, tpa, params from {}.etl_log2 where status ='finished' and task_name !='post-process' and params like 'process_type=med%' or params like '%process_type=med%' order by process_time desc limit 1".format(tenant)
        elif feed.lower() == 'rx':
            query = "select tenantid,tpa, params from {}.etl_log2 where status ='finished' and task_name !='post-process' and params like 'process_type=rx%' or params like '%process_type=rx%' order by process_time desc limit 1".format(tenant)
        else:
            return 'Feed Not Found!'
        try:
            result = query_from_db('trino', query, False)
            if len(result) == 0:
                result_list.append("Manual check: " + tenant)
            else:
                result_list.append(result)
        except Exception as e:
            result_list.append(e)
    return result_list

def extract_info(pieces, info_dict=defaultdict(list), manual_check_list= []):
    error_check_list= []
    for p in pieces:
        try:
            if p[0] =='M':
                manual_check_list.append(p)
            else:
                tenantid = p[0][0]
                for i in p[0][2].split('|'):
                    if i.startswith('yaml_file='):
                        yaml = i
                        info_dict[yaml].append(tenantid)
        except Exception:
            error_check_list.append(p)
    return info_dict, manual_check_list

def convert_to_df(info_list, type):
    df = pd.DataFrame(info_list.items(), columns=['YAML', 'Tenants'])
    df.insert(0,"Type",type)
    return df

def combine_df(elig_df, rx_df, med_df):
    frames = [elig_df, rx_df, med_df]
    return pd.concat(frames)
