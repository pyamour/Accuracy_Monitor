#!/usr/bin/env python
# coding: utf-8

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


class Empty_File_S3():
    
    def __init__(self,bucket="testinternalrealmaster",exclude=[],condo=False):
        self.bucket = bucket
        self.exclude = exclude
        self.condo = condo
       
    def get_list(self):
        exclude = self.exclude
        condo = self.condo
        cmdln = "aws s3 ls s3://" + self.bucket +"/  --region us-east-2 "
        p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        files=[]
        for line in p.stdout.readlines():
            #print(line)
            if True:
                #print(re.findall(r"Listing#\d+\.csv.gz",str(line)))
                if not condo:
                    file = re.findall(r"\b\D+#\d+\.csv.gz",str(line))
                else:
                    file = re.findall(r"\b\D+#\d+_condo\.csv.gz",str(line))
                if (len(file)>0) and (file[0].strip() not in exclude):
                    files.append(file[0].strip())
                else:
                    if not condo:
                        file = re.findall(r"\b\D+#\d+\.csv",str(line))
                    else:
                        file = re.findall(r"\b\D+#\d+_condo\.csv",str(line))
                    if (len(file)>0) and (file[0].strip() not in exclude):
                        files.append(file[0].strip())
        return files
    
    def delete(self,files):
        for i, file in enumerate(files): 
            cmdln = "aws s3 rm s3://" + self.bucket + "/" + file + "  --region us-east-2 "
            print(cmdln)
            p = subprocess.Popen(cmdln, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                print(line)
