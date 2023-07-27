#=========================================#
#           Import Dependencies           #
#=========================================#
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.webdriver.common.by import By
import os
import pandas as pd

#=========================================#
#               Functions                 #
#=========================================#
def run_driver(path):
    global driver, wait
    # Setting option and run web driver
    options=webdriver.EdgeOptions()
    options.add_argument("--headless")
    driver = webdriver.Edge(path,options=options)
    driver = webdriver.Edge(path)
    wait = WebDriverWait(driver, 10)
    driver.get("https://www.twitter.com/login")

def login(username,password):
    # Put username
    user = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,"input[autocomplete='username']")))
    user.send_keys(username)
    next_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,"div[class='css-18t94o4 css-1dbjc4n r-sdzlij r-1phboty r-rs99b7 r-ywje51 r-usiww2 r-2yi16 r-1qi8awa r-1ny4l3l r-ymttw5 r-o7ynqc r-6416eg r-lrvibr r-13qz1uu']")))
    next_button.click()
    sleep(0.5)
    # put password
    passw = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,"input[autocomplete='current-password']")))
    passw.send_keys(password)
    login = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,"div[data-testid='LoginForm_Login_Button']")))
    login.click()
    sleep(0.5)

def advance_search(key):
    # take to the explore page
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='explore']"))).click()
    sleep(0.5)
    # search key
    search = wait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[data-testid='SearchBox_Search_Input']")))
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.DELETE)
    search.send_keys(key)
    search.send_keys(Keys.ENTER)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#react-root > div > div > div.css-1dbjc4n.r-18u37iz.r-13qz1uu.r-417010 > main > div > div > div > div > div > div.css-1dbjc4n.r-aqfbo4.r-gtdqiz.r-1gn8etr.r-1g40b8q > div.css-1dbjc4n.r-1e5uvyk.r-5zmot > div:nth-child(2) > nav > div > div.css-1dbjc4n.r-1adg3ll.r-16y2uox.r-1wbh5a2.r-1pi2tsx.r-1udh08x > div > div:nth-child(2)"))).click()
    sleep(0.5)

def is_page_bottom_reached():
    return driver.execute_script(
    "return (window.innerHeight + window.scrollY) >= document.body.scrollHeight"
)

def get_data():
    tweets = []
    # Scroll down until the end of the page is reached
    while not is_page_bottom_reached():
        tweet = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-testid='tweetText']")))
        # Store data to list
        for i in tweet:
            tweets.append(i.text)
        # Scroll down page
        driver.execute_script("window.scrollBy(0,2000)","")
        sleep(2)
    return tweets

#=========================================#
#                Parameter                #
#=========================================#
path = os.environ["path"]
username = os.environ["username"]
password = os.environ["password"]
date_until = "2023-01-01"
date_from = '2023-03-31'
search_key = f"#Gempa (from:infoBMKG) until:{date_until} since:{date_from}"
path_result = os.environ["path_result"]

#=========================================#
#              Main Process               #
#=========================================#
run_driver(path)
login(username,password)
advance_search(search_key)
get_data()
# Calling DataFrame constructor on list
df = pd.DataFrame(tweets,columns=['tweet_bmkg'])
df = df.drop_duplicates()
df.to_csv(path_result,sep=";",index=False)