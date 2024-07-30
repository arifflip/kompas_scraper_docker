#|----------------------------------------------------------------------------------|  
#  Dependecies 
#|----------------------------------------------------------------------------------|

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
from datetime import datetime
#from tqdm import tqdm
import json
import time

#|----------------------------------------------------------------------------------|  
#  Scraper Class 
#|----------------------------------------------------------------------------------|
class scraper:

  #instances
  def __init__(self, url):
      self.url = url

  def connection_test(self) :

    print(f'*Checking connection to this {self.url} url')
    time.sleep(2)

    response=requests.get(self.url)
    status=response.status_code
    if status != 200 :
      print('*There is problem with the connection')
      status='Failed to Connect'
    else :
      print('*Connected')
      status='Connected'

    return status
  
  def visit_and_exctract_html(self) : 

    """
    visit_and_exctract_html function work by deliver request to the page using requests library
    then reformat it in bs4 format.
    This function return bs4 format.
    """
    #checking response status

    response=requests.get(self.url)
    soup = BeautifulSoup(response.text,"html.parser")
    return soup

  def cleanse_date(self,txt) :
    """
    This method works by replacing certain character then reforrmat it to thi %Y-%m-%d %H:%M:00 datettime format.
    Error handling will handle the error with filling variable with None value
    """
    try :
      txt=txt.replace(',','').replace(' WIB','')
      new_date = datetime.strptime(txt, '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:00')
    except :
      new_date = None
    return new_date

  def extract_news_tag(self,elem) :
    """
    This method works by selecting class 'article__subtitle' then extract its text value.
    Error handling will handle the error with filling variable with None value
    """
    try :
      value=elem.find('div',class_='article__subtitle').text
    except :
      value=None
    return value

  def extract_news_story_url(self,elem) :
    """
    This method works by selecting a tag then extract its href which contain an url.
    Error handling will handle the error with filling variable with None value
    """
    try :
      value=elem.find('a','article__link').get('href')
    except :
      value=None
    return value

  def extract_news_story_title(self,elem) :
    """
    This method works by selecting class 'article__link' then extract its text value.
    Error handling will handle the error with filling variable with None value
    """
    try :
      value=elem.find('a','article__link').text
    except :
      value=None
    return value

  def extract_news_release_date(self,elem) :
    """
    This method works by selecting class 'article__date' then extract its text value (the cleansig process uses cleanse_date method).
    Error handling will handle the error with filling variable with None value
    """
    try :
      value=elem.find('div', class_='article__date').text
      clean_value=self.cleanse_date(value)
    except :
      clean_value=None
    return clean_value

  def remove_junk_words(self,txt) :
    """
    This method works by replacing words that labeled as common word or junks /word that has no meaning.
    list_common contains common word which taken by task description and I add few of words in it.
    """
    #ccommon list obtained from task and i personally add 'yang'+etc because it is occcur as one of most common words
    list_common=["dan","dari","di","dengan","ke","oleh","pada","sejak","sampai","seperti","untuk","buat","bagi","akan","antara","demi","hingga"
    ,"kecuali","tentang","seperti","serta","tanpa","kepada","daripada","oleh karena itu","antara","dengan","sejak","sampai","bersama","beserta"
    ,"menuju","menurut","sekitar","selama","seluruh","bagaikan","terhadap","melalui","mengenai","yang","merupakan","anda","itu","the","tidak",'ini','dalam','apa']

    #loop each list_common for word rreplacement in text, all common word will be transformed to ''
    for i in list_common :
      txt=txt.replace(' '+i+' ','')

    return txt


  def get_most_common_words(self,url) :
    """
    This method extract most common word using word_count method.
    """
    #get the response from url
    soup=BeautifulSoup(requests.get(url).text,"html.parser")

    #get the content and cleanse it
    text=soup.find('div', class_='read__content').text
    text=text.replace('\n',' ').replace('\xa0','').lower()
    text=self.remove_junk_words(text)

    #count most common words and return it as list
    result=dict(sorted(self.word_count(text).items(), key=lambda item: item[1])[-5:]) #pick only top 5
    result=list(result.keys())[::-1] #convert to list and reverse its orderr
    return result

  def word_count(self,str):
    """
    This method works using dictionary to save counted word (key=word,value=total occurance)
    """
    # dictinoary to hold name of word and its total value and text variable as a list from splitted text
    dictholder = dict()
    text = str.split()

    # loop each word in text and assign the word to dict by
    for kata in text:
        if kata in dictholder:
            dictholder[kata] += 1
        else:
            dictholder[kata] = 1
    return dictholder

  #def json_maker(self,ls_,ls_2,ls_3,)

  def run_scrapper(self) :
    """
    run_scrapper method work by performing loop for each news inside class 'article__list clearfix'.
    Values that extracted are story_url/news_url, story_tag, story_release_date, story_title
    and articel text (include most common words using get_most_common_words methode).
    """
    #visit the page and transforrm it to bs object
    soup=self.visit_and_exctract_html()

    #get all bs objecct and storee it in list all_news
    all_news=soup.find_all("div", class_="article__list clearfix")[0:10]

    #create list to store all scraped value dict
    list_result=[]

    #loop each object inside all_news list to extract its value
    #for news in tqdm(all_news, desc="*Scraping Progress") :
    for news in all_news :

      #dictionaru to hold scrape vvalue
      dict_value={}

      #scrape and insert dynamic scrape time to dict
      dict_value["scrape_time"]=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

      #scrape and insert dynamic story source to dict
      dict_value["story_source"]='kompas_'+self.url.split('/')[-1].replace('+','_')

      #scrape and insert story news tag to dict to dict
      dict_value["story_news_tag"]=self.extract_news_tag(news)

      #scrape and insert story news tag to dict
      dict_value["story_release_date"]=self.extract_news_release_date(news)

      #scrape and insert story news title to dict
      dict_value["story_title"]=self.extract_news_story_title(news)

      #scrape and insert story news url to dict
      dict_value["story_url"]=self.extract_news_story_url(news)

      #scrape and insert most common words to dict
      dict_value["most_common_words"]=self.get_most_common_words(self.extract_news_story_url(news))

      #append all dict to list result
      list_result.append(dict_value)

    #convert it to json
    #result_scraper=json.dumps(list_result, indent=4)
    return list_result
