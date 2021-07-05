# -*- coding: utf-8 -*-

import rauth
import requests
import pandas as pd
import bs4
import urllib2
import lxml
import re
import time
from pandas.io.json import json_normalize
import numpy as np
 
###########################
### proxy setup
###########################


proxy_support = urllib2.ProxyHandler({"https":"http://216.227.130.3:80"})
opener = urllib2.build_opener(proxy_support)
urllib2.install_opener(opener)

my_ip = urllib2.urlopen('https://wtfismyip.com/text').read()
print my_ip


###########################
### scraper functions 
###########################

#find other reviews for a given user
def users_other_reviews(user_id):
    if user_id.find(' ')<0: 
        rating_ = []
        use_url = 'https://www.tripadvisor.com/members/' + user_id 
        response = urllib2.urlopen(use_url)
        soup = bs4.BeautifulSoup(response.read(), "lxml")
        all_revs = soup.find_all('img',class_='sprite-ratings')
        for i in range(0, len(all_revs)):
            try: 
                rating_.append( float(all_revs[i].attrs['content']))
            except: 
                continue
            
    
        return sum(rating_)/len(rating_)
    else: 
        return -1
        
# trip advisor scraper
def get_ta_one_page(use_url): 
    
    stars, dates, descs, authors, rev_cnts, url_str, status, user_loc, user_id, user_avgs = [], [], [], [], [], [], [], [], [], []

    response = urllib2.urlopen(use_url)
    soup = bs4.BeautifulSoup(response.read(), "lxml")
    
    hrefs_upper = soup.find_all ('div' ,class_="quote")
    
    for i,rev in enumerate(hrefs_upper): 
         
        try:      
            #time.sleep(2)
            full_text_url = "https://www.tripadvisor.com/" + str(rev.find('a')['href'])
            response = urllib2.urlopen(full_text_url)
            soup = bs4.BeautifulSoup(response.read(), "lxml")
            full_text = soup.find ("p", property="reviewBody").get_text().strip()
            rating = int(soup.find ("img", class_="sprite-rating_s_fill")['alt'][:1])
            author = soup.find ("span", class_="expand_inline").get_text()
            rev_cnt  = int(re.sub('[^0-9]','',soup.find ("span", class_="badgeText").get_text()[:6]))
            
            has_date1 =  soup.find_all ("span", class_="ratingDate relativeDate")
            
            if len(has_date1)>0: 
                date = soup.find ("span", class_="ratingDate relativeDate")['title']
            else:
                date = soup.find("span","ratingDate").get_text()[9:].strip()

            user_loca = soup.find ("div", class_="location").get_text().strip()
            user_avg = users_other_reviews (author)
        except: 
            continue
        
        descs.append(full_text)    
        authors.append(author)    
        rev_cnts.append(rev_cnt)
        url_str.append(full_text_url)
        stars.append(float(rating))
        status.append('not hidden')
        user_loc.append(user_loca)
        user_id.append(author)
        dates.append(date)
        user_avgs.append(user_avg)


    # Put the ratings and dates into a Pandas dataframe for analysis
    data = pd.DataFrame({'date': [pd.to_datetime(date) for date in dates], 
                         'rating': stars, 
                         'desc': descs,
                         'author': authors, 
                         'num_revs' : rev_cnts, 
                         'url' : url_str, 
                         'user_loc': user_loc, 
                         'user_id': user_id,
                         'review_status' : status, 
                         'author_avgs' : user_avgs })  

    return data      
        
    
    

def scrape_data_ta(main_url, start_page ):

    response = urllib2.urlopen(main_url)
    soup = bs4.BeautifulSoup(response.read(), "lxml")
 
    tot_reviews = int(re.sub('[^0-9]','',soup.find('h3',class_='tabs_header reviews_header').get_text()))

    if start_page > 0: 
        starting = start_page
    else: 
        starting = 0

    frames = []
    
    for page_start in range(starting,tot_reviews, 10):

        print(page_start)
        this_url = "https://www.tripadvisor.com/Hotel_Review-g38815-d274374-Reviews-or" + str(page_start) +"--Great_Wolf_Lodge_Kansas_City-Kansas_City_Kansas.html#REVIEWS"
        this_dat = get_ta_one_page(this_url)
        frames.append(this_dat)
        
 
    return pd.concat(frames)


############################
## example location 1 - kansas city
############################   

#check IP 
import socket
socket.gethostbyname(socket.gethostname())    

ks = scrape_data_ta(main_url='https://www.tripadvisor.com/Hotel_Review-g38815-d274374-Reviews-Great_Wolf_Lodge_Kansas_City-Kansas_City_Kansas.html#REVIEWS')
ks .reset_index()

nodups = ks.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\trip_advisor_gw_reviews_kansas_city.csv", encoding="utf-8", index=False, mode='w')


