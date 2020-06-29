#!/usr/bin/python

import json;


def get_job(path):
    return job(path)

class job:
    def __init__(self, path):
        self.path = path

    def __str__(self):
        print(self.path)
        
    ''' Query Name '''
    def get_query(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            query_entry= next((item for item in properties if item["name"] == "hive.query.string"), ({'value': 'null'}))
            if query_entry['value'] != 'null': 
                return {'query': query_entry['value']}; 
        
    def get_all(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            return json.dumps(ResJson, indent=4);
        
    def get_input_dir(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            input_dir_entry = next((item for item in properties if item["name"] == "mapreduce.input.fileinputformat.inputdir"), ({'value': 'null'}))
            if input_dir_entry['value'] != 'null': 
                return {'input_dir' : input_dir_entry['value'].split(',')};
        
    ''' Query Name '''
    def get_external_location(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            location_entry= next((item for item in properties if item["name"] == "location"), ({'value': 'null'})) 
            if location_entry['value'] != 'null':
                return {'external_location' : location_entry['value'].split(',')};
        
    ''' Execution Plan'''
    def get_execution_plan(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            execution_plan = next((item for item in properties if item["name"] == "hive.exec.plan"), ({'value': 'null'}))
            return execution_plan['value']
        
    def get_workflow_meta(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            workflow_meta_entry = next((item for item in properties if item["name"] == "mapreduce.workflow.id"), ({'value': 'null'}))
            
            workflow_meta = {'framework': '',
                    'username': '',
                    'date': '',
                    'workflowid': ''}
            if workflow_meta_entry['value'] != 'null':
                epe_ls = workflow_meta_entry['value'].split("_")
                workflow_meta['framework'] = epe_ls[0]
                workflow_meta['username'] = epe_ls[1]
                workflow_meta['date'] = epe_ls[2]
                workflow_meta['workflowid'] = epe_ls[3]
                return workflow_meta;
        
    def get_user_name(self):
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            execution_plan = next((item for item in properties if item["name"] == "mapreduce.job.user.name"), ({'value': 'null'}))
            return execution_plan['value']
        
    def get_all_tasks(self):
        with open(self.path+"/tasks/tasks.json", 'r') as json_file:
            ResJson= json.load(json_file)
            return json.dumps(ResJson, indent=4);


    def get_all_conf(self):
        configs = {}
        with open(self.path+"/conf.json", 'r') as json_file:
            ResJson= json.load(json_file)
            properties = ResJson['conf']['property']
            for item in properties:
                configs[item['name'].encode("utf-8")] =  item['value'].encode("utf-8")
        return configs

'''
#   print(json.dumps(properties, indent=4))
   for item in properties:
     print(item["name"] + " ; " + item["value"])

   """ Job Name """
   job_name= next(item for item in properties if item["name"] == "mapreduce.job.name")
   print("job name is: " + job_name['value'])

   """ Query Name """
   query_name= next(item for item in properties if item["name"] == "hive.query.string")
   print("Hive query is:" + query_name['value'])

   """ INPUT Directory """
#   input_dir= next(item for item in properties if item["name"] == "pig.input.dirs")
#   print("input directory is: " + input_dir['value'])


   """ OUTPUT Directory """
#   output_dir= next(item for item in properties if item["name"] == "pig.reduce.output.dirs")
#   print("output directory is: " + output_dir['value'])


   """ GETTING SIZES """
#   command = "http://sp-hd-1:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/counters"
#   resp = requests.get(command)
#   ResJson= json.loads(resp.content.decode('utf-8'))
#   properties = ResJson['jobCounters']['counterGroup']


   """ INPUT Size """
#   counter = next(item for item in properties if item["counterGroupName"] == "org.apache.hadoop.mapreduce.FileSystemCounter")
#   counter = counter['counter']

#   input_size = next(item for item in counter if item["name"] == "HDFS_BYTES_READ")
#   print("input size is: %d"  % input_size['totalCounterValue'])


   """ OUTPUT Size """
#   output_size= next(item for item in counter if item["name"] == "HDFS_BYTES_WRITTEN")
#   print("output size is: %d" % output_size['totalCounterValue'])
''' 
