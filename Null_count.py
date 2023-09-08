import pandas as pd
import os
from tqdm import tqdm
import csv
from collections import defaultdict
#os.chdir('AE')
def valueret():
    return 'str'

csv.field_size_limit(100000000)
dtypedict = defaultdict(valueret)
count=0
files = ['AE_22_8_23_1_final.csv']
unique_set=set()
for file in files:
    #f = open(file, 'r')
    #csvreader = csv.DictReader(f)
    #for record in tqdm(csvreader):
    #    if record['Competitor_Id'] != None:
    #        count+=1
    #df_null = pd.DataFrame()
    count = 0
    for index,chunks in tqdm(enumerate(pd.read_csv(file,header=0,usecols=["Competitor_Id","Currency","seller_code","Website_Name","Prime","country","Product_Name","Brand","Product_URL","Category_Path","navigation_path","leaf_node_url","Image_URL","Bestseller_tag","Bestseller_L1","Bestseller_L2","List_Price","Promo_Price","Discount","Offer_count","Selected_Variant","Stock_Information","Product_Rating","Product_Review","Product_Condition","seller_ranking","Shipping_Price","Shipping_Details","seller_note","Seller_Name","fulfillment_type","fulfillment_value","Stock","Specifications","Product_Details","AMAZON_Choice_Flag","Coupons_Details","Shipping_Type","Seller_URL","Seller_details","Seller_rating","Seller_reviews","Timestamp","Buybox_Flag","expected_sla_maximum","expected_sla_minimum","international_image","PDP_tags","Parent_Asin","FBT"],chunksize=10000, encoding='utf-8', dtype=dtypedict,engine='python', na_values=[None], error_bad_lines=False),start=1)):
        if index==1:
            df_null = chunks.isnull().sum()
            # print(df_null)
               
            continue
        
        count += len(chunks)
        df_null2 = chunks.isnull().sum()
        df_null = df_null.add(df_null2)
        #if index==2:
        #    break
        # print(df_null)
        
        unique_set.update(set(chunks["Competitor_Id"].to_list()))
        #count+=len(chunks)
  
    print(f'Total count for {file}:\n ',len(unique_set),'\n',count,'\n', df_null)
    #df_dict = {'Unique_Cat_Paths':list(unique_set)}
    #df_dict = pd.DataFrame.from_dict(df_dict)
    df_null.to_csv(f'{file}_null_count.csv', index=True)
    unique_set=set()