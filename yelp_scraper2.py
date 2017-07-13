
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 18:12:18 2016

@author: Matt McKenna
"""

import rauth
import requests
import pandas as pd
import bs4
import urllib2
import lxml
import time
from pandas.io.json import json_normalize


###########################
### proxy setup
###########################

proxy_support = urllib2.ProxyHandler({"https":"http://216.227.130.3:80"})
opener = urllib2.build_opener(proxy_support)
urllib2.install_opener(opener)

#test proxy
my_ip = urllib2.urlopen('https://wtfismyip.com/text').read()
print my_ip

#########################################
#Part 2: Use addresses to search Yelp API
#########################################

#setup my yelp API credentials
consumer_key="iTuZ9s0ZILmh3syZ7u_zdQ"
consumer_secret="FJgo9jfDbZ4yk7eqkrAgXaeJGP4"
access_token_key="KM5VcUikApMOn-bRtIO_W1t6os0F_QgS"
access_token_secret="d6YqZUM7O-ZXfOZpPixaMPinnPA"

#setup session
session = rauth.OAuth1Session(
    consumer_key = consumer_key
    ,consumer_secret = consumer_secret
    ,access_token = access_token_key
    ,access_token_secret = access_token_secret)


#function to get results from Yelp Business API  
def get_results_business(params):
      
  #make API request   
  url_str = "https://api.yelp.com/v2/business/" + str(params)
  request = session.get(url_str)

  #Transforms the JSON API response into a Python dictionary
  data = request.json()
  session.close()

  return data  

  
#######################################  
#function for all other user's reviews
#######################################


def users_other_reviews(user_id, n_user_revs):
    time.sleep(4)
    
    rating_ = []
            
    for page_start in range(0, n_user_revs, 10):
        time.sleep(2)
        use_url = 'https://www.yelp.com/user_details_reviews_self?userid=' + user_id + '&rec_pagestart=' + str(page_start)
        response = urllib2.urlopen(use_url)
        soup = bs4.BeautifulSoup(response.read(), "lxml")
        n_revs_this_page = len(soup.find_all('i',class_='star-img'))
        for j in range(0,n_revs_this_page):
            ratings = float(soup.find_all('i',class_='star-img')[j].attrs['title'][:3])
            rating_.append(ratings)
                                                
            if len(rating_)==0 or sum(rating_)==0:
                ret_ = 0
            else: 
                ret_ = sum(rating_) / len(rating_) 

    #return reduce(lambda x, y: x + y, rating_) / len(rating_)    
    return ret_    
    

##################################################
#Part 3: Scrape the ratings, dates of ratings,
#        and reviews from Yelp website (not API)
##################################################

def list_to_str(x): 
    new_str = str(x.to_string().split(" ", 1)[1].lstrip())
    new_str.replace(u'\xa0', u' ')
    return(new_str)
    
#scrape all reviews from Yelp

def scrape_data(biz, str2, end_page):
    time.sleep(3)
    """ Accepts a business entry from Yelp's API, scrapes rating data 
        from the business' Yelp page """
        
    stars, dates, descs, authors, rev_cnts, url_str, status, user_loc, user_id, user_avgs = [], [], [], [], [], [], [], [], [], []
    
    use_id = str(biz['id'].to_string().split(" ", 1)[1].lstrip())
    
    n_reviews = biz['review_count']
    n_pages = n_reviews/20 + 1
    
    if end_page>0: 
        ending = end_page
    else: 
        ending=n_pages*20
      
    #for page_start in range(0,2*20, 20):
    for page_start in range(0,ending , 20):
        url = 'http://www.yelp.com/biz/{}?start={}&sort_by=date_desc'.\
               format(str2, page_start)
               #format(page_start)

                                
        response = urllib2.urlopen(url)
        
        soup = bs4.BeautifulSoup(response.read(), "lxml")

        revs = soup.find_all('div', itemprop = 'review')  
        print(len(revs))
        for i,rev in enumerate(revs): 
            try:
                desc = rev.find(itemprop='description').get_text()
                author = soup.find_all('a', class_='user-display-name')[i].get_text()
                rev_cnt = soup.find_all('li',class_='review-count')[i].get_text().strip().split(' ', 1)[0]  
                date = soup.find_all('meta', itemprop='datePublished')[i].attrs['content']
                rating = soup.find_all('meta',itemprop='ratingValue')[1:][i].attrs['content']
                user_loca = soup.find_all('li',class_='user-location responsive-hidden-small')[i].get_text().strip()
                user_ids = soup.find_all('div',class_='review review--with-sidebar')[i].attrs['data-signup-object'][8:]
                #user_avg = users_other_reviews (user_ids, int(rev_cnt))

            except AttributeError:
                continue    
            descs.append(desc)    
            authors.append(author)    
            rev_cnts.append(rev_cnt)
            dates.append(date)
            url_str.append(url)
            stars.append(float(rating))
            status.append('not hidden')
            user_loc.append(user_loca)
            user_id.append(user_ids)
            #user_avgs.append(user_avg)
    
    # Put the ratings and dates into a Pandas dataframe for analysis
    data = pd.DataFrame({'date': [pd.to_datetime(date) for date in dates], 
                         'rating': stars, 
                         'desc': descs,
                         'author': authors, 
                         'num_revs' : rev_cnts, 
                         'url' : url_str, 
                         'user_loc': user_loc, 
                         'user_id': user_id,
                         #'user_avg': user_avgs, 
                         'review_status' : status})
    return data

####################################    
#function for the hidden reviews
####################################

def scrape_data_hidden(biz):
    time.sleep(5)
    """ Accepts a business entry from Yelp's API, scrapes rating data 
        from the business' Yelp page """
        
    stars, dates, descs, authors, rev_cnts, url_str, status, user_loc, user_id, user_avgs = [], [], [], [], [], [], [], [], [], []
   
    url_hidden = 'https://www.yelp.com/not_recommended_reviews/' + biz + '?not_recommended_start=10'
    print(url_hidden)
    
    response = urllib2.urlopen(url_hidden)
    soup = bs4.BeautifulSoup(response.read(), "lxml")

    num_not_rec_revs = int(soup.find_all('div', class_ = "ysection not-recommended-reviews review-list-wide")[0].get_text()[:50].strip()[:2])
  
    for page_start in range(0, num_not_rec_revs, 10):
    #for page_start in range(0, 20, 10):
        
        #construct URL
        url = 'https://www.yelp.com/not_recommended_reviews/' + biz + '?not_recommended_start=' + str(page_start)
        print('url is: ')
        print(url)
        
        response = urllib2.urlopen(url)
        soup = bs4.BeautifulSoup(response.read(), "lxml")

        revs = soup.find_all('div', class_ = 'review-wrapper')  
        print(len(revs))
        for i,rev in enumerate(revs): 
            try:
                desc=soup.find_all('p')[(i+1)].get_text().strip()
                #print(desc)
                author = soup.find_all('span', class_='user-display-name')[i].get_text()
                print(author)
                rev_cnt = soup.find_all('li',class_='review-count')[i].get_text().strip().split(' ', 1)[0]  
                date = soup.find_all('span', class_='rating-qualifier')[i].get_text()
                rating = float(soup.find_all('i',class_='star-img')[i].attrs['title'][:3])  
                user_loca = soup.find_all('li',class_='user-location responsive-hidden-small')[i].get_text().strip()
                user_ids = soup.find_all('div',class_='review review--with-sidebar')[i].attrs['data-signup-object'][8:]
                #user_avg = users_other_reviews (user_ids, int(rev_cnt))
                
            except: 
                continue
            
          
            descs.append(desc)    
            authors.append(author)    
            rev_cnts.append(rev_cnt)
            dates.append(date)
            url_str.append(url)
            stars.append(float(rating))
            status.append('hidden')
            user_loc.append(user_loca)
            user_id.append(user_ids)
            #user_avgs.append(user_avg)
     
    # Put the ratings and dates into a Pandas dataframe for analysis
    data = pd.DataFrame({'date': [pd.to_datetime(date) for date in dates], 
                         'rating': stars, 
                         'desc': descs,
                         'author': authors, 
                         'num_revs' : rev_cnts, 
                         'url' : url_str, 
                         'user_loc': user_loc, 
                         'user_id': user_id,
                         #'user_avg': user_avgs, 
                         'review_status' : status})
    return data 


def run_location ( gw_yelp_id, ending_ ): 
    
    #loop thru all Yelp locations and get reviews Yelp ID
    #use the ID to get all the reviews for the location
 
    frames = []

    try: 
        
        business_res = json_normalize(get_results_business(gw_yelp_id))
        loc_data = scrape_data(business_res, gw_yelp_id, ending_ )
        loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
        #setup some columns we need
        loc_data['yelp_business_id']  = gw_yelp_id  
        loc_data_hidden['yelp_business_id'] = gw_yelp_id

        frames.append(loc_data)
        frames.append(loc_data_hidden)
        
        #find out how many hidden reviews the location has
        main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
        #hid_response = urllib2.urlopen(main_page)   
        
    #write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
    except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


    #combind all reviews for all locations into one dataframe
    all_locs = pd.concat(frames)
    all_locs.reset_index()

    nodups = all_locs.drop_duplicates()
    
    return(nodups)


#poconos

yelp_new_poco = run_location('great-wolf-lodge-scotrun', 30)


#write results to csv for analysis in R
yelp_new_poco.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_schlitterbahn_tx.csv", encoding="utf-8", index=False, mode='w')




'36576fe2a962f38289752a3aa1c9ed91'


# read data and send to aylien

send = pd.read_csv ("C:/projects/startrack/gavagai/ta_poconos_out.csv" )






























#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "hawaiian-falls-pflugerville-pflugerville"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_hawaiin_falls.csv", encoding="utf-8", index=False, mode='w')



#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "coco-key-water-resort-kansas-city"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_coco_key.csv", encoding="utf-8", index=False, mode='w')



#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "worlds-of-fun-kansas-city"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_worlds_of_fun.csv", encoding="utf-8", index=False, mode='w')


#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "schlitterbahn-kansas-city-waterpark-kansas-city"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_schlitterbahn_ks.csv", encoding="utf-8", index=False, mode='w')




#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "kalahari-resorts-and-conventions-pocono-manor"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_kalahari.csv", encoding="utf-8", index=False, mode='w')



#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "camelback-resort-tannersville-3"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_camelback.csv", encoding="utf-8", index=False, mode='w')




#wetn-wild-emerald-pointe-water-park-greensboro

logf = open("gw_download.log", "w")
frames = []

try: 
    
    gw_yelp_id = "split-rock-resort-lake-harmony"
    business_res = json_normalize(get_results_business(gw_yelp_id))
    loc_data = scrape_data(business_res,gw_yelp_id )
    loc_data_hidden = scrape_data_hidden(gw_yelp_id)
   
    #setup some columns we need
    loc_data['yelp_business_id']     = gw_yelp_id  
    loc_data_hidden['yelp_business_id']     = gw_yelp_id

    #dont care about these for now
    #loc_data['avg_first_5'] = loc_data['ratings'][:5].mean()
    #loc_data['avg_last_5' ] = loc_data['ratings'][-5:].mean()

    frames.append(loc_data)
    frames.append(loc_data_hidden)
        
    #find out how many hidden reviews the location has
    main_page = 'https://www.yelp.com/biz/'+gw_yelp_id+'?sort_by=date_desc'
    #hid_response = urllib2.urlopen(main_page)   
        
#write any error locations to the log (these are usually caused by phone # mismatches between MapQuest and Yelp)    
except (KeyError, IndexError): 
        pass
        logf.write('\n something went wrong getting Yelp API review data')


#combind all reviews for all locations into one dataframe
all_locs = pd.concat(frames)
all_locs.reset_index()

nodups = all_locs.drop_duplicates()

#write results to csv for analysis in R
nodups.to_csv("c:\\projects\\great_wolf_lodge\\yelp_gw_reviews_split_rock.csv", encoding="utf-8", index=False, mode='w')

