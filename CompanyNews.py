import requests
import feedparser
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
# import urllib3.contrib.pyopenssl

import datetime
from dateutil import parser
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication
# from email.header import Header
# urllib3.contrib.pyopenssl.inject_into_urllib3()
import sys
import os
import yfinance as yf
import json

import xml.etree.ElementTree as ET


timestamp = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d-%H%M")
# get current working directory
# current_path = os.getcwd()

# sys.path.append(r'D:\Onedrive\OneDrive - IHS Markit\15_Archives\Tools & Templates\PythonModules')
# from config import SendEmail

class News():
    
    def __init__(self) -> None:
        '''
        Get the lists of sources and keywords
        '''
        # self.__current_path = os.getcwd()
        self.__current_path = os.path.abspath(r'C:\Users\Kunfeng.Zhu\OneDrive - IHS Markit\General - Data collection automation working team')

        self.tickers = pd.read_excel(os.path.join(self.__current_path,'Oil and Gas - Keywords and Ticket codes.xlsx'), sheet_name='Ticker Codes') 
        self.feeds = pd.read_excel(os.path.join(self.__current_path,'Oil and Gas - Keywords and Ticket codes.xlsx'), sheet_name='RSS feeds')

        # self.feeds = pd.read_excel(os.path.join(self.__current_path,'Oil and Gas - Keywords and Ticket codes.xlsx'), sheet_name='RSS feeds').head(5) # for testing purpose
        # self.tickers = pd.read_excel(os.path.join(self.__current_path,'Oil and Gas - Keywords and Ticket codes.xlsx'), sheet_name='Ticker Codes').head(10) # for testing purpose

        self.keywords = pd.read_excel(os.path.join(self.__current_path,'Oil and Gas - Keywords and Ticket codes.xlsx'), sheet_name='Keywords')

        # self.sources.set_index('Source',inplace = True)

        # self.feeds = self.sources.RSS.dropna()
        # self.websites = self.sources.Website.dropna()
        # self.keywords = ['carbon capture', 'CCS', 'CCUS', 'carbon', 'energy transition']


    def WebPageParse(self, url, keywords):
        '''
        search the url in the source/url list based on keywords
        '''
        df = pd.DataFrame()
        # Make a request to the website
        response = requests.get(url, verify=False) 
        soup = bs(response.text, 'html.parser')
        links = soup.find_all('a')
        # Search for the keywords in the HTML content
        for keyword in keywords:
            if keyword in soup.get_text():
                # print(f"Found '{keyword}' on {website}")
                df = pd.concat([df, pd.Series({'source':self.source,'keyword':keyword, 'link':url, 'content':soup.get_text()}),], axis=1)
        return df, links

    def getPages(self,websites,keywords):
        df = pd.DataFrame()
        for source, link in websites.items():
        #     df['source']=source
        #     df['link']=link
            df2, links = self.WebPageParse(source,link, keywords)
            df=pd.concat([df,df2], axis=1)
        df = df.T.set_index('source')
        return df

    def YahooFinanceNews(self):
        '''
        get yahoo news by tickers
        '''
        news_list = pd.DataFrame()
        no_data_list = pd.DataFrame()
        tickers = self.tickers.set_index('Exchanges')
        tickers['Ticker codes'] = tickers['Ticker codes'].apply(str) # convert to string if it's a number ticker
        for exchange, ticker in tickers.iterrows():
            code = ticker['Ticker codes']
            tk = yf.Ticker(code)
            # company_news_list = tk.news
            company_news_list = tk.news
            company_news = pd.DataFrame()
            
            try:
                for news in company_news_list:
                    news_entry = pd.Series(news)
                    # news_entry = news_entry.append(pd.Series({'company':company})) 
                    news_entry = pd.concat([news_entry, pd.Series({'exchange':exchange})], axis=0)
                    news_entry = pd.concat([news_entry, pd.Series({'searched_ticker':code})], axis=0)
                    company_news=pd.concat([company_news,news_entry], axis=1)
                    # company_news['searched_ticker'] = ticker['Ticker codes']
            except:
                no_data_list=pd.concat([no_data_list,pd.Series(ticker, index=[exchange])])
            news_list=pd.concat([news_list,company_news], axis=1)
            # news_list = news_list.T.reset_index('company')
        self.yahoo_news = news_list.T.set_index('exchange')
        self.no_data_list = no_data_list
        return self.yahoo_news
    
    def RssFeeds(self):
        '''
        Get the updated RSS feed data
        '''
        # self.feeds = self.feeds.head(3)
        feeds_data = pd.DataFrame()
        for feed in self.feeds.loc[self.feeds.Feeds.notna()].Feeds:
            try:
                news = feedparser.parse(feed)
                feed_data = pd.DataFrame()
                for entry in news.entries:
                    s = pd.Series({'title':entry.title, 'link':entry.link, 'description':entry.description, 'published':entry.published})
                    feed_data = pd.concat([feed_data, s], axis=1)
                feeds_data=pd.concat([feeds_data, feed_data], axis=1)
            except:
                pass
        self.feeds_data = feeds_data.T 
        # News.save(feeds_data, 'News from feeds')
        #the description is in html - convert it into text
        try:
            self.feeds_data.description = self.feeds_data.description.apply(lambda x: bs(x,'html.parser').get_text())
            self.feeds_data.published = self.feeds_data.published.apply(lambda x: parser.parse(x).strftime("%Y-%m-%d %H:%M:%S")) # parse the date format
        except:
            pass

        return self.feeds_data

    def filter_by_keywords(self):
        '''
        filter the returned rss news and Yahoo news by keywords
        '''
        keywords = list(self.keywords.Keywords) # convert the keywords series into a list to iterate through
        keywords = [x.lower() for x in keywords] # convert all to lower case for comparison

        # feeds_data['full_description']=feeds_data.title + feeds_data.description
        self.feeds_data['title']=self.feeds_data.title.str.lower() # convert all title to lower case
        # self.feeds_data.full_description = self.feeds_data.full_description.str.lower()
        self.feeds_data['description'] = self.feeds_data.description.str.lower() # convert description into lower case
        # reset index
        self.feeds_data.reset_index(inplace=True)
        self.feeds_data.drop('index', axis=1, inplace=True)
        # set a filter column, which is False by default, and will be true when keyword is in the text
        self.feeds_data['filters']=False
        # set a keywords column, which is empty and the keywords encountered will be added
        self.feeds_data['keywords in title'] =''
        self.feeds_data['keywords in description'] =''
        self.yahoo_news['filters']=False
        self.yahoo_news['keywords']=''
        self.yahoo_news.title = self.yahoo_news.title.str.lower()
        for keyword in keywords:

            # search keyword in title
            self.feeds_data['temp']= self.feeds_data.title.apply(lambda x: News.__apply_keyword(keyword,x)) # True if there is key words in the title
            self.feeds_data.loc[self.feeds_data.temp==True,'keywords in title']= self.feeds_data.loc[self.feeds_data.temp==True,'keywords in title'] + ',' + keyword # append the keyword
            self.feeds_data['filters']=  self.feeds_data.temp | self.feeds_data.filters # set filter to be true if there is a keyword in title

            # search keyword in description
            self.feeds_data['temp']= self.feeds_data.description.apply(lambda x: News.__apply_keyword(keyword,x))
            self.feeds_data.loc[self.feeds_data.temp==True,'keywords in description']= self.feeds_data.loc[self.feeds_data.temp==True,'keywords in description'] + ',' + keyword
            self.feeds_data['filters']=  self.feeds_data.temp | self.feeds_data.filters
            
            # search keyword in yahoo news title
            self.yahoo_news['temp'] = self.yahoo_news.title.apply(lambda x: News.__apply_keyword(keyword,x))
            self.yahoo_news.loc[self.yahoo_news.temp==True,'keywords']=self.yahoo_news.loc[self.yahoo_news.temp==True,'keywords']+','+keyword
            self.yahoo_news.filters =  self.yahoo_news.temp | self.yahoo_news.filters

        
        # trim the keywords column
        # self.feeds_data['keywords in title'].apply(str.strip)
        self.feeds_data['keywords in title']= self.feeds_data['keywords in title'].apply(lambda x: str.strip(str(x))[1:]) # remove the first ','
        self.feeds_data['keywords in description']=self.feeds_data['keywords in description'].apply(str.strip)
        self.feeds_data['keywords in description']=self.feeds_data['keywords in description'].apply(lambda x: str(x)[1:])
        self.yahoo_news.keywords=self.yahoo_news.keywords.apply(str.strip)
        self.yahoo_news.keywords=self.yahoo_news.keywords.apply(lambda x: str(x)[1:])

        # create table with filtered entries only
        self.filtered_feeds_data = self.feeds_data.loc[self.feeds_data.filters==True]
        self.filtered_yahoo_news = self.yahoo_news.loc[self.yahoo_news.filters==True]
        # remove the temporary columns
        self.filtered_feeds_data = self.filtered_feeds_data.drop(['temp','filters'], axis=1)
        self.filtered_yahoo_news = self.filtered_yahoo_news.drop(['temp','filters'], axis=1)
        try:
            self.yahoo_news.providerPublishTime = self.yahoo_news.providerPublishTime.apply(datetime.datetime.fromtimestamp)
        except:
            pass
        self.yahoo_news = self.yahoo_news.drop(['temp','filters'], axis=1)
        self.feeds_data = self.feeds_data.drop(['temp','filters'], axis=1)

        # concatenation with historical data
        historical_feeds_data, historical_yahoo_news = self.__get_recent_excelfile()
        self.cumulative_feeds_data = pd.concat([historical_feeds_data,self.feeds_data], axis=0)
        self.cumulative_yahoo_news = pd.concat([historical_yahoo_news,self.yahoo_news], axis=0)
        # drop duplicated rows
        self.cumulative_feeds_data.drop_duplicates(subset='title', inplace=True)
        self.cumulative_yahoo_news.drop_duplicates(subset='title', inplace=True)

    @staticmethod
    def __apply_keyword(keyword, string):
        '''
        test whether a string contains the keyword
        '''
        keyword = str(keyword)
        string = str(string)
        if keyword in string:
            return True
        else:
            return False

    # @staticmethod
    # def __html_to_text(html):
    #     soup = bs(html,'html.parser')
    #     text = soup.get_text()
    #     return text

    def __get_recent_excelfile(self):
        '''
        This hidden method is to get the recent exported file
        '''
        folder_path = os.path.join( self.__current_path, 'Company news folder')
        recent_file = None
        recent_time = datetime.datetime.min
        for filename in os.listdir(folder_path):
            if filename.endswith(".xlsx"):  # check if the file is an Excel file
                file_path = os.path.join(folder_path, filename)
                mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                if mod_time > recent_time:
                    recent_time = mod_time
                    recent_file = file_path
        try:
            cuml_rss_data = pd.read_excel(recent_file, sheet_name='cuml feeds data')
            cuml_yahoo_news = pd.read_excel(recent_file, sheet_name='cuml yahoo news')
        except:
            cuml_rss_data = pd.DataFrame()
            cuml_yahoo_news = pd.DataFrame()
        return cuml_rss_data,cuml_yahoo_news

    @staticmethod
    def save(df, file_name):
        '''
        This is only used to save dataset separately
        '''
        location = os.path.join(os.path.dirname(os.path.abspath('__file__')),'Company news folder')
        df.to_excel(f"{location}/{file_name} {timestamp}.xlsx")
    
    def __unpivot_keywords(self):
        '''
        unpivot the keywords for the convenience of filtering
        '''
        # unpivot the keywords in feeds_data title
        feeds_data = self.feeds_data
        feeds_data = pd.concat([feeds_data, feeds_data['keywords in title'].str.split(',',expand=True)], axis=1)
        feeds_data_title_keyword_unpivoted = feeds_data.melt(id_vars=feeds_data.columns[:5],value_vars=feeds_data.columns[6:],value_name='keyword in title',ignore_index=False)
        feeds_data_title_keyword_unpivoted = feeds_data_title_keyword_unpivoted.replace('',np.nan) # replace the empty string as np.nan, so dropna() can be applied
        feeds_data_title_keyword_unpivoted.dropna(subset='keyword in title',inplace=True)
        feeds_data_title_keyword_unpivoted.drop(['keywords in title', 'variable'], axis=1, inplace=True) # remove the un-used columns
        # feeds_data_title_keyword_unpivoted.published = feeds_data_title_keyword_unpivoted.published.apply(pd.to_datetime).apply(lambda x: x.replace(tzinfo = None))

        # unpivot the keywords in feeds_data description
        feeds_data = self.feeds_data
        feeds_data = pd.concat([feeds_data, feeds_data['keywords in description'].str.split(',',expand=True)], axis=1)
        feeds_data_descr_keyword_unpivoted = feeds_data.melt(id_vars=feeds_data.columns[:6],value_vars=feeds_data.columns[6:],value_name='keyword in description',ignore_index=False)
        feeds_data_descr_keyword_unpivoted = feeds_data_descr_keyword_unpivoted.replace('',np.nan) # replace the empty string as np.nan, so dropna() can be applied
        feeds_data_descr_keyword_unpivoted.dropna(subset='keyword in description',inplace=True)
        feeds_data_descr_keyword_unpivoted.drop(['keywords in description', 'keywords in title','variable'], axis=1, inplace=True) # remove the un-used columns
        # feeds_data_descr_keyword_unpivoted.published = feeds_data_descr_keyword_unpivoted.published.apply(pd.to_datetime)
        # feeds_data_descr_keyword_unpivoted.published = feeds_data_descr_keyword_unpivoted.published.apply(lambda x: x.replace(tzinfo = None)) # remove timezone information to avoid the error when wirting into Excel: Excel does not support datetimes with timezones. Please ensure that datetimes are timezone unaware before writing to Excel.


        #unpivot the keywords in yahoo_news
        # yahoo_news = self.yahoo_news.reset_index()
        yahoo_news = self.yahoo_news
        yahoo_news = pd.concat([yahoo_news,yahoo_news.keywords.str.split(',', expand=True)], axis=1)
        yahoo_news_unpivoted = yahoo_news.melt(id_vars=yahoo_news.columns[:(yahoo_news.columns.get_loc('keywords')+1)], value_vars=yahoo_news.columns[(yahoo_news.columns.get_loc('keywords')+1):], value_name='keyword', ignore_index=False)
        yahoo_news_unpivoted =   yahoo_news_unpivoted.replace('', np.nan).sort_index()
        yahoo_news_unpivoted = yahoo_news_unpivoted.dropna(subset=['keyword'], axis=0)
        yahoo_news_unpivoted = yahoo_news_unpivoted.drop(['keywords','variable'], axis=1)
        try:
            yahoo_news_unpivoted.providerPublishTime = yahoo_news_unpivoted.providerPublishTime.apply(datetime.datetime.fromtimestamp)
        except:
            pass

        # unpivot the company tickers
        # yahoo_news_unpivoted = pd.concat([yahoo_news_unpivoted, yahoo_news_unpivoted.relatedTickers.apply(pd.Series)], axis=1)
        # yahoo_news_unpivoted = yahoo_news_unpivoted.melt(id_vars= yahoo_news_unpivoted.columns[:10], value_vars=yahoo_news_unpivoted.columns[10:], value_name='company',ignore_index=False)
        # yahoo_news_unpivoted.replace('',np.nan)
        # yahoo_news_unpivoted.dropna(subset='keyword', inplace=True)
        # yahoo_news_unpivoted.drop('variable', axis=1, inplace=True)

        # remove duplated news
        feeds_data_keyword_unpivoted = pd.concat([feeds_data_descr_keyword_unpivoted, feeds_data_title_keyword_unpivoted], axis=0)
        # feeds_data_keyword_unpivoted = feeds_data_keyword_unpivoted.drop_duplicates()
        # yahoo_news_unpivoted = yahoo_news_unpivoted.drop_duplicates(subset=list(yahoo_news_unpivoted.columns).remove('thumbnail'))

        # assign the results as a class variable
        self.feeds_data_keywords_unpivoted = feeds_data_keyword_unpivoted
        self.yahoo_news_unpivoted = yahoo_news_unpivoted


    def save_files(self):
        '''
        This is used to save both the yahoo and rss news in a single excel file
        '''
        file = os.path.join(self.__current_path,'Company news folder', f"News data collected on {timestamp}.xlsx")
        with pd.ExcelWriter(file) as writer:
            self.feeds_data.to_excel(writer, sheet_name="RSS news", index=False)
            self.yahoo_news.to_excel(writer, sheet_name="Yahoo news")
            # self.no_data_list.to_excel(writer, sheet_name="No data companies")
            self.feeds_data_keywords_unpivoted.to_excel(writer, sheet_name = 'RSS news by keyword', index=False)
            self.yahoo_news_unpivoted.to_excel(writer, sheet_name = 'Yahoo news by keyword')
            self.cumulative_feeds_data.to_excel(writer, sheet_name='cuml feeds data' , index = False)
            self.cumulative_yahoo_news.to_excel(writer, sheet_name='cuml yahoo news')

    def update(self):
        '''
        batch running the methods to update the data
        '''
        self.RssFeeds()
        self.YahooFinanceNews()
        self.filter_by_keywords()
        self.__unpivot_keywords()
        self.save_files()

def opml_feeds_converter(file):
    '''
    convert the opml file exported from feedly into a list of feeds
    '''
    # Load the OPML file
    opml_file = file
    tree = ET.parse(opml_file)
    # Get the root element
    root = tree.getroot()

    # Find all 'outline' elements
    outlines = root.findall('.//outline')
    rss_feeds = []
    for outline in outlines:
        if outline.get('type') == 'rss':
            rss_feeds.append(outline.get('xmlUrl'))
    return rss_feeds

def main():
    News().update()



if __name__=="__main__":
    main()
    # NewOutput = feeds.reset_index().to_json()
    # print(NewOutput)


