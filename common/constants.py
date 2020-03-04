model_date = ["2019-01-11","2019-03-05","2019-03-19"]
launch_date = ["2019-01-11","2019-03-14","2019-03-26","2019-04-05","2019-04-15","2019-04-22"]
HS_LABEL = ['Detached', 'Semi-Detached', 'Duplex', 'Triplex', 'Fourplex', 'Link', 'Att/Row/Twnhouse', 'Det Condo',
            'Semi-Det Condo', 'Condo Townhouse']
CD_LABEL = ['Condo Apt', 'Comm Element Condo', 'Co-Ownership Apt', 'Co-Op Apt', 'Leasehold Condo', 'Vacant Land Condo',
            'Condo Apartment', 'Phased Condo']
exclude_list = ["Sold#20190305211532.csv.gz","Listing#20190316092234.csv.gz","Listing#20190317030001.csv.gz","Listing#20190324030001.csv.gz","Sold#20190328091644.csv.gz","Sold#20190328205309.csv.gz"]
fraud_id = ['TRBS4398907',"TRBW4387431","TRBN4380052","TRBN4400154","TRBC4399153","TRBW4421347","TRBN4402155","TRBW4449366"]
datalake = "/media/qindom-cpu/wd2/datalake/"
cnnstr = "mysql+pymysql://foodhwy:Liangzizhineng@8888@192.168.9.114:3306/fds_pro"
