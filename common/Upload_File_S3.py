#Access key ID = 'AKIAJWILCZEKUOJXDYIQ'
#Secret access key = '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'
#above is just for QA
import subprocess
import time
import os
import time
import re
import sys
from IPython.display import clear_output


class Upload_File_S3():
    
    def __init__(self,path = '/home/jimmy/FDS/Data/Online-Results/internalrealmaster/',interval=30,num_begin="20190214000000",num_end="20190228240000"):
        self.path = path
        self.interval = interval
        self.num_begin = num_begin
        self.num_end = num_end
       
    def get_list(self):
        #sort files to be upload
        Listing_files= os.listdir(self.path + "/Listing/")
        #print(files)
        for i, file in enumerate(Listing_files): 
            if not os.path.isdir(file): 
                if 'Listing' in str(file) or "Sold" in str(file):
                    nums = re.findall(r"\d+",file)
                    #print(nums)
                    if len(nums) > 0:
                        if (nums[0] > self.num_begin) and (nums[0] < self.num_end):
                            Listing_files[i] = nums[0]
                        else:
                            Listing_files[i] = ""
                else:
                    Listing_files[i] = ""
            else:
                Listing_files[i] = ""
        Sold_files= os.listdir(self.path + "/Sold/")
        #print(files)
        for i, file in enumerate(Sold_files): 
            if not os.path.isdir(file): 
                if 'Listing' in str(file) or "Sold" in str(file):
                    nums = re.findall(r"\d+",file)
                    #print(nums)
                    if len(nums) > 0:
                        if (nums[0] > self.num_begin) and (nums[0] < self.num_end):
                            Sold_files[i] = nums[0]
                        else:
                            Sold_files[i] = ""
                else:
                    Sold_files[i] = ""
            else:
                Listing_files[i] = ""
        files = Sold_files + Listing_files
        #print(Sold_files)
        #print(Listing_files)
        #print(files)
        files = list(dict.fromkeys(files))
        files.sort()
        try:
            files.remove("")
        except:
            print("")
            
        return files
    
    def upload(self,files):
        path = self.path
        interval = self.interval
        for i, file in enumerate(files): 
            if os.path.isfile(path + "Sold/Sold#" + file + ".csv.gz"):
                #load sold file and wait until it has been processed or 30 minutes passed
                cmdln = "aws s3 cp " + path + "Sold/Sold#" + file + ".csv.gz" + " s3://testrealmaster  --region us-east-2 "
                p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                    clear_output()
                    print(line)
                #if in shard or error
                wait_time = 0
                finished = False
                while wait_time < interval:
                    time.sleep(60)
                    wait_time = wait_time + 1
                    cmdln = "aws s3 ls s3://testinternalrealmaster/shard/  --region us-east-2 "
                    p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    for line in p.stdout.readlines():
                        clear_output()
                        print(line)
                        if ("Sold#" + file) in str(line):
                            finished = True
                    if finished:
                        break
                    cmdln = "aws s3 ls s3://testrealmaster/  --region us-east-2 "
                    p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    for line in p.stdout.readlines():
                        clear_output()
                        print(line)
                        if ("Error#Sold#" + file) in str(line):
                            finished = True
                    if finished:
                        break
            if os.path.isfile(path + "Listing/Listing#" + file + ".csv.gz"):
                #upload listing file and wait until it has been processed or 30 minutes passed
                cmdln = "aws s3 cp " + path + "Listing/Listing#" + file + ".csv.gz" + " s3://testrealmaster  --region us-east-2 "
                p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                    clear_output()
                    print(line)
                #if predict or error
                wait_time = 0
                finished = False
                while wait_time < interval:
                    time.sleep(60)
                    wait_time = wait_time + 1
                    cmdln = "aws s3 ls s3://testrealmaster/  --region us-east-2 "
                    p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    for line in p.stdout.readlines():
                        clear_output()
                        print(line)
                        if ("Predict#" + file) in str(line):
                            finished = True
                        if ("Error#Listing#" + file) in str(line):
                            finished = True
                    if finished:
                        break

