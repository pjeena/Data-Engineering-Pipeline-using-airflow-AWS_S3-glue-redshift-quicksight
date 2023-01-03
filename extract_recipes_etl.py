#from mpi4py import MPI
#comm = MPI.COMM_WORLD
#my_rank = comm.Get_rank()
#p = comm.Get_size()

from bs4 import BeautifulSoup
import requests
import pandas as pd 
import numpy as np
import warnings
import time
import re
pd.set_option('display.max_columns', None)

def run_recipes_etl():
    my_rank = 56
    urls_working = pd.read_csv('..../data/raw/working_urls_final.csv')
    urls_working = urls_working['0']
    urls = list(urls_working.values)
    delta = 5
    urls = urls[my_rank*delta:(my_rank+1)*delta]


    def url_extraction(url):
        """
        Takes an url of a category in the website allrecipes.com and returns the urls of all the recipes on the page
        """
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        s1 = soup.find_all('a',{'class':'comp mntl-card-list-items mntl-document-card mntl-card card card--no-image'})
    #  s2 = soup.find_all('a',{'class':'comp card--image-top mntl-card-list-items mntl-document-card mntl-card card card--no-image'})
        s = s1 
        urls=[]
        for i in s:
            urls.append(i.get('href'))  
                
        return(urls)




    def titles(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_title = soup.find_all('h1',{'class':'comp type--lion article-heading mntl-text-block'})
        if not list(s_title):
            return 'None'
        else:
            return s_title[0].get_text().strip()
        
    def sub_category(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_subcategory_tag = soup.find_all('span',{'class': 'link__wrapper'})
        if not list(s_subcategory_tag):
            return 'None'
        else:
            return s_subcategory_tag[7].get_text()  
        

    def description(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_description = soup.find_all('h2',{'class': 'comp type--dog article-subheading'})
        if not list(s_description):
            return 'None'
        else:
            return s_description[0].get_text().strip()
        
        
        
        
    def ratings(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_rating = soup.find_all('div',{'class':'comp type--cat-bold mntl-recipe-review-bar__rating mntl-text-block'})
        if not list(s_rating):
            return 'None'
        else:
            return s_rating[0].get_text().strip() 
        
        
    def ratings_count(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_rating_count = soup.find_all('div',{'class':'comp type--cat mntl-recipe-review-bar__rating-count mntl-text-block'})
        if not list(s_rating_count):
            return 'None'
        else:
            return s_rating_count[0].get_text().strip() 
        
        
        
        
        
        
    def reviews(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_reviews = soup.find_all('div',{'class':'comp type--squirrel-link mntl-recipe-review-bar__comment-count mntl-text-block'})
        if not list(s_reviews):
            return 'None'
        else:
            return s_reviews[0].get_text().strip()
        
    def num_photos(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')
        
        s_photos = soup.find_all('div',{'class':'comp type--squirrel-link dialog-link recipe-review-bar__photo-count mntl-text-block'})
        if not list(s_photos):
            return 'None'
        else:
            return s_photos[0].get_text().strip()  
        
    
        
    def time_taken_and_num_servings(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')    
        s_total_time = soup.find_all('div',{'class' : 'mntl-recipe-details__content'})
        
        if not list(s_total_time):
            return ['None']
        else:
            contents = s_total_time[0].get_text().strip().split('\n\n')
            popp = []
            for i in contents:
                popp.append(re.sub('\n','', i).split(':'))
        
            dict_contents = dict(zip(list( np.array(popp)[:,0]) , list(np.array(popp)[:,1]) ))
            return dict_contents
        
        
    def ingredients(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')    
        ingredients = []
        for ing in soup.select(".mntl-structured-ingredients__list-item"):
            ingred = ing.text.strip()
            ingredients.append(ingred)
            
        if not ingredients:
            return ['None']
        else:
            return ingredients
        
        
    def num_steps(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')    
        s_steps = soup.find_all('li',{'class' : "comp mntl-sc-block-group--LI mntl-sc-block mntl-sc-block-startgroup"})
            
        if not list(s_steps):
            return 'None'
        else:
            return len(s_steps)
        
        
    def macros(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')    
        s_macros = soup.find_all('td',{'class':'mntl-nutrition-facts-summary__table-cell type--dog-bold'})
            
        if not list(s_macros):
            return ['None']*4
        else:
            macros_per_serving = []
            for mac in s_macros:
                macros_per_serving.append(mac.get_text().strip())
            return macros_per_serving
        

        
        
        

    def nutrients(url):
        data = requests.get(url)
        soup = BeautifulSoup(data.content,'html.parser')    
        s_nutrients = soup.find_all('tr')

        if not list(s_nutrients):
            return ['None']
        else:
            nutrients = []
            for nut in s_nutrients:
                nutrients.append(nut.get_text().strip().split('\n')[0:2])
            dict_nutrients = dict(zip(list( np.array(nutrients[8:])[:,0]) , list(np.array(nutrients[8:])[:,1]) ))
            dict_nutrients['Calories'] = nutrients[0][0]
    #        dict_nutrients['Servings'] = nutrients[5][1]
            return dict_nutrients


        
    #urls = urls[0:5]

    start = time.time()

    titles_all = []
    for link in urls: 
        titles_all.append(titles(link)) 




    sub_category_all = []
    for link in urls: 
        sub_category_all.append(sub_category(link)) 

        
    description_all = []
    for link in urls: 
        description_all.append(description(link)) 
        
        
    ratings_all = []
    for link in urls: 
        ratings_all.append(ratings(link)) 
        

    ratings_count_all = []
    for link in urls: 
        ratings_count_all.append(ratings_count(link)) 
        
        
    reviews_all = []
    for link in urls: 
        reviews_all.append(reviews(link)) 
        
        
    num_photos_all = []
    for link in urls: 
        num_photos_all.append(num_photos(link)) 
        

        
    time_taken_all = []
    servings_all = []
    for link in urls:
    #    print(link, count)
        
        get_content = time_taken_and_num_servings(link)
        if get_content !=  ['None'] and 'Total Time' in get_content:
            time_taken_all.append(time_taken_and_num_servings(link)['Total Time'])
        else:
            time_taken_all.append('None')
        
            
        if get_content !=  ['None'] and 'Servings' in get_content:
            servings_all.append(time_taken_and_num_servings(link)['Servings'])
        else:
            servings_all.append('None')         
            
        
    ingredients_all = []
    for link in urls:
    #    print(link, count)
        
        get_content = ingredients(link)
        if get_content !=  ['None']:
            ingredients_all.append(ingredients(link))
        else:
            ingredients_all.append('None')     

            
    num_steps_all = []
    for link in urls: 
        num_steps_all.append(num_steps(link)) 
        

        

        
    fats_all = []
    saturated_fats_all = []
    cholesterol_all = []
    sodium_all = []
    carbohydrates_all = []
    fibers_all = []
    sugars_all = []
    proteins_all = []
    vitamin_all = []
    calcium_all = []
    iron_all = []
    potassium_all = []
    calories_all = []
    #servings_all = []

    #count = 0
    for link in urls:
    #    print(link, count)
        
        get_content = nutrients(link)
        if get_content !=  ['None'] and 'Total Fat' in get_content:
            fats_all.append(get_content['Total Fat'])
        else:
            fats_all.append('None')

        if get_content !=  ['None'] and 'Saturated Fat' in get_content:
            saturated_fats_all.append(get_content['Saturated Fat'])
        else:
            saturated_fats_all.append('None')        
            
        if get_content !=  ['None'] and 'Cholesterol' in get_content:
            cholesterol_all.append(get_content['Cholesterol'])
        else:
            cholesterol_all.append('None')         
            
            
        if get_content !=  ['None'] and 'Sodium' in get_content:
            sodium_all.append(get_content['Sodium'])
        else:
            sodium_all.append('None')

        if get_content !=  ['None'] and 'Total Carbohydrate' in get_content:
            carbohydrates_all.append(get_content['Total Carbohydrate'])
        else:
            carbohydrates_all.append('None')
            
            
        if get_content !=  ['None'] and 'Dietary Fiber' in get_content:
            fibers_all.append(get_content['Dietary Fiber'])
        else:
            fibers_all.append('None')        
            
        if get_content !=  ['None'] and 'Total Sugars' in get_content:
            sugars_all.append(get_content['Total Sugars'])
        else:
            sugars_all.append('None')
            
            
        if get_content !=  ['None'] and 'Protein' in get_content:
            proteins_all.append(get_content['Protein'])
        else:
            proteins_all.append('None')
            
        if get_content !=  ['None'] and 'Vitamin C' in get_content:
            vitamin_all.append(get_content['Vitamin C'])
        else:
            vitamin_all.append('None')
            
            
        if get_content !=  ['None'] and 'Calcium' in get_content:
            calcium_all.append(get_content['Calcium'])
        else:
            calcium_all.append('None')
            
            
        if get_content !=  ['None'] and 'Iron' in get_content:
            iron_all.append(get_content['Iron'])
        else:
            iron_all.append('None')
            
        if get_content !=  ['None'] and 'Potassium' in get_content:
            potassium_all.append(get_content['Potassium'])
        else:
            potassium_all.append('None')
            
            
        if get_content !=  ['None'] and 'Calories' in get_content:
            calories_all.append(get_content['Calories'])
        else:
            calories_all.append('None')
            
    #    count = count +1


    end = time.time()
    print(end - start)



    df = pd.DataFrame(
        {'Name': titles_all,
        'URL' : urls,
        'Category': 'Fruits, Vegetables and Other Produce',
        'Description': description_all,
        'Ratings out of 5' : ratings_all,
        'Ratings count' : ratings_count_all,
        'Reviews' : reviews_all,
        'Photos' : num_photos_all,
        'Cooking time' : time_taken_all,
        'Calories' : calories_all,
        'Servings' : servings_all, 
        'Ingredients' : ingredients_all,
        'no of steps' : num_steps_all,     
        'Fats' : fats_all,
        'Saturated Fat' : fats_all,
        'Cholesterol' : fats_all,
        'Sodium' : sodium_all,
        'Carbs' : carbohydrates_all,
        'Fibers' : fibers_all,
        'Sugars' : sugars_all,
        'Proteins' : proteins_all,
        'Vitamins' : vitamin_all,
        'Calcium' : calcium_all,
        'Iron' : iron_all,
        'Potassium' : potassium_all    
        })


    df.to_csv('s3://allrecipes-data/recipe_%s.csv' %(my_rank),index=False)



