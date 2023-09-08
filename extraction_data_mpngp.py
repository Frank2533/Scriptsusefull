import concurrent.futures
import copy

import re
from collections import defaultdict
from datetime import datetime
import json
import math
import pickle
import time
import traceback
import socket
from pymongo import InsertOne
import scrapingbee
from tqdm import tqdm
import numpy as np
import datetime as dt
import configparser
import random

from pymongo import MongoClient

# import unitdataclass
from urllib import request as urlreq
# import RawConfigParser
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
from lxml import html
import logging
import os
import csv
quote=csv.QUOTE_ALL

def get_db(db, tbnme):
	global conn_output
	global conn_input
	conn_output = MongoClient('mongodb://10.100.18.39:36018/')
	conn_output_db = conn_output[db]
	conn_output = conn_output_db[tbnme]
	conn_input = conn_output_db['Noon']
	return conn_output

def get_old_mpn():
	global mpnset
	mpnset = pickle.load(open('AE_uni_mpns.pkl','rb'))
	


def get_records(conn_output):
	# global conn_output
	# mpn = []
	records = conn_output.find({},{'_id':0, 'Cache_page URL':0}, batch_size=50)
	return records

def check_amazon(fulfill_value):
	if fulfill_value is not None:
		if 'amazon' in fulfill_value.lower():
			if any(['us' in fulfill_value.lower(), 'uk' in fulfill_value.lower(), 'global' in fulfill_value.lower(), 'sa' in fulfill_value.lower(), 'eg' in fulfill_value.lower()]):
				return True
			else:
				return False

def check_sellernote(sellernote):
	if (sellernote is not None) and ('international' in sellernote.lower() or 'ships from outside' in sellernote.lower() or 'import' in sellernote.lower()):
		return True
	else:
		return False


def check_sla(slamax, slamin):
	if slamin is not None:
		if int(slamin)>7:
			return True
		else:
			return False
	else:
		return False



def converternoon(record):
	# if record["Sku_id"]=="B0C3VFCZL3":
	#	  pass
	global mpnset


	timestamp = record['Timestamp']
	# print(timestamp)
	timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
	timestamp = datetime.strftime(timestamp, '%m/%d/%Y %H:%M:%S')
	if len(record['FBT_dict'])==0:
		record['FBT_dict']=None
	if '-' in timestamp:
		print(timestamp)
	try:
		record['Extra2|Stock Information']
	except KeyError:
		record['Extra2|Stock Information'] = record['Extra2|Stock_Information']
	try:
		if record['navpath_catcrawl'] == "None":
			record['navpath_catcrawl'] = "Electronics>Hi-Fi & Home Audio>Accessories>Remote Controls"
	except Exception as e:
		print(e)
	try:
		if record["Seller_Offer_Details"] is None:
			record["Number_Of_offers|Offer_Count"]=0
		else:
			record["Number_Of_offers|Offer_Count"]=len(record["Seller_Offer_Details"])
	except Exception as e:
		print(e)
	outputlistrecord = []
	if record['pdp_tags'] is not None and len(record['pdp_tags'])==0:
		record['pdp_tags']=None


	output_record = {'country':'United Arab Emirates',
					 'Sku_id':record['Sku_id'],
					 'Currency':record['Currency'],
					 'Website_Name':record['Site|website_Name'],
					 'Prime':record['Prime|Prime_NonPrime'],
					 'Product_Name':record['Product_Name'],
					 'Brand':record['Brand'],
					 'Product_URL':record['Product_InputUrl|Product_URL'],
					 'Category_Path':record['Category_Path'],
					 'Image_URL':record['Image_url'],
					 'Bestseller_tag':record['Extra1|bestSeller_tag'],
					 'Tax_info':record['Tax_information|TAX_info'],
					 'Offer_count':record['Number_Of_offers|Offer_Count'],
					 # 'Selected Variant':record['selected_variant'],
					 'Stock_Information':record['Extra2|Stock Information'],
					 'Product_Rating':record['Rating|Product_Rating'],
					 'Product_Review':record['Reviews|Product_Review'],
					 'Specifications':record['Specification'],
					 'Competitor_Id':record['Sku_id'],
					 'Product_Description':record['Product_Description_Noon'],
					 'Product_Details':record['Product_Details_Noon'],
					 'AMAZON_Choice_Flag':record['Amazon_Choice_Flag'],
					 'Coupons_Details':record['Offer_text|Coupons_Details'],
					 'Payment_mode':record['Payment_mode'],
					 'Fulfilled_By':record['Fulfilled_By'],
					 'Transaction_details':record['Transaction_details'],
					 'Parent_Asin':record['Parent_Asin'],
					 'Warranty':record['Warranty'],
					 'Timestamp':timestamp,
					 'Original_Url':record['Original_Url'],
					 'PDP_tags':record['pdp_tags'],
					 'FBT':record['FBT_dict'],
					 'navigation_path':record['navpath_catcrawl'],
					 'leaf_node_url':record['navpath_leafnode_url']
					 }
	if record['Sku_id'] in mpnset:
		output_record['scrape_status'] = 1
	else:
		output_record['scrape_status'] = 0
	if len(record['selected_variant'])>0:
		output_record['Selected_Variant']= record['selected_variant']
	else:
		output_record['Selected_Variant'] =None
	if len(record['bestsellerdata'])>0:
		for index,bt in enumerate(record['bestsellerdata'],start=1):
			# if index==1:
			try:
				output_record[f"Bestseller_L{index}"] = record['bestsellerdata'][bt]
			except:
				output_record[f"Bestseller_L{index}"] = None
			if index==2:
				break
	else:
		output_record[f"Bestseller_L1"] = None
		output_record[f"Bestseller_L2"] = None

	if record['Seller_Offer_Details'] is not None:
		temprec = output_record
		if len(record['Seller_Offer_Details'])>0:
			for seller in record['Seller_Offer_Details']:


				if str(seller['Buybox_Flag']) == '1':
					bsellername = seller['Seller_Name']
					bboxprice = seller['Now_Price|Promo Price']
					break
				else:
					bsellername = None
					bboxprice = None
			# if len(record['Seller_Offer_Details'])>4:
				# print("hi")
			for seller in record['Seller_Offer_Details']:
				if 'coimbra' in seller["full_text"].lower():
					seller["full_text"] = 'Ships from'
				if seller['Seller Rating'] is not None and seller['Seller Reviews'] is None:
					seller['Seller Reviews'] = "1"
				temprec = output_record
				temprec['seller_code'] = seller['seller_code']
				temprec['Buybox_Price'] = bboxprice
				temprec['Seller_Price'] = seller['Now_Price|Promo Price']
				temprec['List_Price'] = seller['Was_Price|List Price']
				temprec['Promo_Price'] = seller['Now_Price|Promo Price']
				temprec['Discount'] = seller['Discount']
				temprec['BuyBox_Seller'] = bsellername
				temprec['Seller_Name'] = seller['Seller_Name']
				temprec['seller_ranking'] = seller['Seller_ranking']
				# temprec['Shipping_Price'] = seller['shipping_prices']
				temprec['Shipping_Details'] = seller['shipping_days_text']
				temprec['fulfillment_value'] = seller['Ships From Info']
				temprec['Stock'] = seller['Stock']
				temprec['Shipping_Type'] = seller['Shipping Types']
				temprec['Seller_URL'] = seller['Seller_Url']
				temprec['Seller_details'] = seller['Seller_Name']
				# temprec['Seller_details'] = seller['Seller_details']
				temprec['Seller_rating'] = seller['Seller Rating']
				temprec['Seller_reviews'] = seller['Seller Reviews']
				temprec['Buybox_Flag'] = seller['Buybox_Flag']
				temprec['Product_Condition'] = seller['Product Condition']
				temprec["fulfillment_type"] = seller["full_text"]
				temprec['international_image'] = seller['seller_intimg_status']
				temprec['Discount'] = seller['Discount']
				temprec['Promo_Price'] = seller['Now_Price|Promo Price']
				temprec['seller_note'] = seller['seller_note_txt']
				temprec['expected_sla_maximum'] = seller['Expected_sla_maximum']
				temprec['expected_sla_minimum'] = seller['Expected_sla_minimum']

				if seller['shipping_prices'] and len(seller['shipping_prices'])>0 and ('aed' in seller['shipping_prices'].lower() or 'free' in seller['shipping_prices'].lower()):
					temprec['Shipping_Price'] = seller['shipping_prices'] + 'delivery'
				else:
					temprec['Shipping_Price'] = seller['shipping_prices']



				if temprec['fulfillment_value'] is not None:
					if 'amazon.ae' in temprec['fulfillment_value'].lower() and temprec['Seller_Name'] is None and temprec['seller_code'] is None:
						# print('Done')
						temprec['Seller_Name'] = 'Amazon.ae'
				if seller['shipping_days_text'] is not None and 'tomorrow' in seller['shipping_days_text'].lower():
					temprec['expected_sla_maximum'] = None
					temprec['expected_sla_minimum'] = 1
				else:

					try:
						if seller['Expected_sla_maximum'] is not None and int(seller['Expected_sla_maximum']) < 0:
							temprec['expected_sla_maximum'] = int(seller['Expected_sla_maximum'] + 365)
						else:
							if seller['Expected_sla_maximum'] is not None:
								temprec['expected_sla_maximum'] = int(seller['Expected_sla_maximum'])
						if seller['Expected_sla_minimum'] is not None and int(seller['Expected_sla_minimum']) < 0:
							temprec['expected_sla_minimum'] = int(seller['Expected_sla_minimum'] + 365)
						else:
							if seller['Expected_sla_minimum'] is not None:
								temprec['expected_sla_minimum'] = int(seller['Expected_sla_minimum'])
					except:
						temprec['expected_sla_maximum'] = None
						temprec['expected_sla_minimum'] = None
				# if  temprec['expected_sla_maximum'] is not None:

					if (any([check_amazon(temprec['fulfillment_value']) ,int(temprec['international_image'])==1 ,check_sellernote(temprec['seller_note']) ,check_sla(temprec['expected_sla_maximum'], temprec['expected_sla_minimum'])])) and (temprec['Shipping_Type'] is None):
						temprec['Shipping_Type'] = 'International Shipping'
					else:
						if temprec['Shipping_Type'] is None:
							temprec['Shipping_Type'] = "Local"
						else:
							pass
				copytemprec = copy.deepcopy(temprec)
				# print(temprec)
				outputlistrecord.append(copytemprec)
				del temprec
			del output_record

		else:
			temprec['seller_code'] = None
			temprec['Buybox_Price'] = None
			temprec['Seller_Price'] = None
			temprec['List_Price'] = None
			temprec['Promo_Price'] = None
			temprec['Discount'] = None
			temprec['BuyBox_Seller'] = None
			temprec['Seller_Name'] = None
			temprec['seller_ranking'] = None
			temprec['Shipping_Price'] = None
			temprec['Shipping_details'] = None
			temprec['fulfillment_value'] = None
			temprec['Stock'] = None
			temprec['Shipping_Type'] = None
			temprec['Seller_URL'] = None
			temprec['Seller_Details'] = None
			temprec['Seller_rating'] = None
			temprec['Seller_reviews'] = None
			# temprec['Seller_Code'] = None
			temprec['Buybox_Flag'] = None
			temprec['Product_Condition'] = None
			temprec['expected_sla_minimum'] = None
			temprec['expected_sla_maximum'] = None
			temprec['fulfillment_type'] = None
			temprec['international_image'] = None
			temprec['Discount'] = None
			temprec['Promo_Price'] = None
			temprec['seller_note'] = None
			# temprec['Timestamp'] = record['Timestamp']
			copytemprec = copy.deepcopy(temprec)
			# print(temprec)
			outputlistrecord.append(copytemprec)
			del temprec
			del output_record
	else:
		output_record['seller_code'] = None
		output_record['Buybox_Price'] = None
		output_record['Seller_Price'] = None
		output_record['List_Price'] = None
		output_record['Promo_Price'] = None
		output_record['Discount'] = None
		output_record['BuyBox_Seller'] = None
		output_record['Seller_Name'] = None
		output_record['seller_ranking'] = None
		output_record['Shipping_Price'] = None
		output_record['Shipping_Details'] = None
		output_record['fulfillment_value'] = None
		output_record['Stock'] = None
		output_record['Shipping_Type'] = None
		output_record['Seller_URL'] = None
		output_record['Seller_details'] = None
		output_record['Seller_rating'] = None
		output_record['Seller_reviews'] = None
		# output_record['Seller_Code'] = None
		output_record['Buybox_Flag'] = None
		output_record['Product_Condition'] = None
		output_record['expected_sla_minimum'] = None
		output_record['expected_sla_maximum'] = None
		output_record['fulfillment_type'] = None
		output_record['international_image'] = None
		output_record['Discount'] = None
		output_record['Promo_Price'] = None
		output_record['seller_note'] = None
		# output_record['Timestamp'] = record['Timestamp']
		outputlistrecord.append(output_record)
		del output_record

	return outputlistrecord

def convert_to_noon(records, file):
	global conn_input
	mainrecordlist = []
	bulkreq = []
	files=[]
	count=0
	headers = ['Competitor_Id', 'Currency', 'seller_code', 'Website_Name', 'Prime', 'country', 'Product_Name', 'Brand',
			   'Product_URL', 'Category_Path', 'navigation_path', 'leaf_node_url', 'Image_URL', 'Bestseller_tag',
			   'Bestseller_L1', 'Bestseller_L2', 'List_Price', 'Promo_Price', 'Discount', 'Offer_count',
			   'Selected_Variant', 'Stock_Information', 'Product_Rating', 'Product_Review', 'Product_Condition',
			   'seller_ranking', 'Shipping_Price', 'Shipping_Details', 'seller_note', 'Seller_Name', 'fulfillment_type',
			   'fulfillment_value', 'Stock', 'Specifications', 'Product_Details', 'AMAZON_Choice_Flag',
			   'Coupons_Details', 'Shipping_Type', 'Seller_URL', 'Seller_details', 'Seller_rating', 'Seller_reviews',
			   'Timestamp', 'Buybox_Flag', 'expected_sla_maximum', 'expected_sla_minimum', 'international_image',
			   'PDP_tags', 'Parent_Asin', 'FBT', 'scrape_status']
	continue2=0
	file = file.split('.csv')[0]+'_PART1.csv'
	files.append(file)
	for record in tqdm(records):
		# if continue2==0:
		# 	input("Continue?")
		# 	continue2=1

		outputlist = converternoon(record)
		# print(outputlist)
		mainrecordlist.extend(outputlist)

		if len(mainrecordlist)>200:
			count+=1
			import csv
			if count==1:
				with open(file, 'a', encoding='utf-8', newline='') as file1:
					writer = csv.DictWriter(file1, fieldnames=headers, restval="", extrasaction='ignore', quotechar='"',
											quoting=csv.QUOTE_ALL)
					writer.writeheader()
					writer.writerows(mainrecordlist)
			# for record in mainrecordlist:
			else:
				with open(file, 'a', encoding='utf-8', newline='') as file1:
					writer = csv.DictWriter(file1, fieldnames=headers, restval="", extrasaction='ignore', quotechar='"',
											quoting=csv.QUOTE_ALL)
					# writer.writeheader()
					writer.writerows(mainrecordlist)
			# conn_input.bulk_write(bulkreq)
			# bulkreq.clear()
			mainrecordlist.clear()
			# conn_input.insert_many(mainrecordlist, ordered=False)
			# mainrecordlist.clear()
			if os.path.getsize(file)>3800000000:
				count=0
				file = file.split('.csv')[0]+'_PART2.csv'
				files.append(file)
	if len(mainrecordlist)>0:
		count+=1
		import csv
		if count == 1:

			with open(file, 'a', encoding='utf-8', newline='') as file1:
				writer = csv.DictWriter(file1, fieldnames=headers, restval="", extrasaction='ignore', quotechar='"', quoting = csv.QUOTE_ALL)
				writer.writeheader()
				writer.writerows(mainrecordlist)
			# df = pd.DataFrame.from_records(mainrecordlist, coerce_float=False)
			# df.to_csv(file, mode='a', index=False, columns=headers, quotechar='"', quoting=quote,
			#			)
		# for record in mainrecordlist:
		else:
			with open(file, 'a', encoding='utf-8', newline='') as file1:
				writer = csv.DictWriter(file1, fieldnames=headers, restval="", extrasaction='ignore', quotechar='"',
										quoting=csv.QUOTE_ALL)
				# writer.writeheader()
				writer.writerows(mainrecordlist)


	mainrecordlist.clear()

	import csv
	for file in files:
		with open(file, "r", encoding='utf-8') as input1, open(file.split('.')[0]+"_final.csv", "w", newline='',
																		   encoding='utf-8') as output:
			w = csv.writer(output)
			for record in tqdm(csv.reader(input1)):
				w.writerow(tuple(' '.join(s.strip().replace("\n", '').replace('\u200e', '').split()) for s in record))
	count=0

	def valueret():
		return 'str'

	csv.field_size_limit(100000000)
	dtypedict = defaultdict(valueret)
	count = 0
	files = [file.split('.')[0]+"_final.csv" for file in files]
	unique_set = set()
	for file in files:

		count = 0
		for index, chunks in tqdm(enumerate(pd.read_csv(file, header=0,
														usecols=["Competitor_Id", "Currency", "seller_code",
																 "Website_Name", "Prime", "country", "Product_Name",
																 "Brand", "Product_URL", "Category_Path",
																 "navigation_path", "leaf_node_url", "Image_URL",
																 "Bestseller_tag", "Bestseller_L1", "Bestseller_L2",
																 "List_Price", "Promo_Price", "Discount", "Offer_count",
																 "Selected_Variant", "Stock_Information",
																 "Product_Rating", "Product_Review",
																 "Product_Condition", "seller_ranking",
																 "Shipping_Price", "Shipping_Details", "seller_note",
																 "Seller_Name", "fulfillment_type", "fulfillment_value",
																 "Stock", "Specifications", "Product_Details",
																 "AMAZON_Choice_Flag", "Coupons_Details",
																 "Shipping_Type", "Seller_URL", "Seller_details",
																 "Seller_rating", "Seller_reviews", "Timestamp",
																 "Buybox_Flag", "expected_sla_maximum",
																 "expected_sla_minimum", "international_image",
																 "PDP_tags", "Parent_Asin", "FBT",'scrape_status'], chunksize=10000,
														encoding='utf-8', dtype=dtypedict, engine='python',
														na_values=[None], error_bad_lines=False), start=1)):
			if index == 1:
				df_null = chunks.isnull().sum()
				# print(df_null)

				continue

			count += len(chunks)
			df_null2 = chunks.isnull().sum()
			df_null = df_null.add(df_null2)
			# if index==2:
			#	 break
			# print(df_null)

			unique_set.update(set(chunks["Competitor_Id"].to_list()))
		# count+=len(chunks)

		print(f'Total count for {file}:\n ', len(unique_set), '\n', count, '\n', df_null)
		# df_dict = {'Unique_Cat_Paths':list(unique_set)}
		# df_dict = pd.DataFrame.from_dict(df_dict)
		df_null.to_csv(f'{file}_null_count.csv', index=True)
		unique_set = set()

def get_total(conn_output):
	total_count = conn_output


if __name__ == '__main__':
	listdb = [
				("Amazon_Productcrawl_AE_Electronics", "Productcrawl_1_8_23_data1", "AE_31_8_23.csv")
			  # ("Productcrawl", "AE_Prod_Noon_brand_output_batch_2", "AE2_brand.csv"),
			  # ("Productcrawl", "AE_Product_Noon_brand_output_missing", "AE3_brand.csv"),
			  # ("
				]
	get_old_mpn()
	for names in listdb:
		db, tbnme, file = names
		conn_output = get_db(db, tbnme)
		records = get_records(conn_output)
		convert_to_noon(records, file)
		