import pandas as pd
import numpy as np


df_original = pd.read_csv('...data/raw/all_recipes_raw.csv') 
df = df_original.copy()

# dropping duplicates
df = df.drop_duplicates().reset_index(drop=True)


## dividing recipes into veg, non-veg or vegan
non_veg_tags = ['beef', 'fish','lamb','lambs', 'mutton','meatball','meatballs','pork','poultry', 'sausage',
                'sausages','seafood', 'chicken','wings','meat','salmon','salmons','cob','cobs',
                'ham','pork','kebab','kebabs','snake','gosht','bacon','mutton','lamb','egg','eggs',
               'shrimp', 'shrimps','seafood','buffalo','calf','horse','moose','rabbit','turtle','duck',
                'liver','goose','turkey','jamon','salami','venison','boar','bison','pheasant',
               'prawn','tuna','bass','catfish','caviar','clam','clams','crab','crabs','hot dog','hotdog','Flounder',
                'Lobster', 'lobsters', 'mussel','Mussels','octopus','quail','Scallops', 'Oyster', 'oysters',
                'shark','Smelt','squid','swordfish','Tilapia','Trout','Whitefish','Whiting' ,'steak','veal',
                'Ribs','Brisket', 'lasagna']
non_veg_tags = [i.lower() for i in non_veg_tags]



meal_tags = []
for name in df['Name']:
    name_splitted = name.split()
    status = 'v'
    for word in name_splitted:
        if word.lower() in non_veg_tags:
            status = 'nv'
            
    if status == 'nv':
        meal_tags.append('Non Veg')
    else :
        meal_tags.append('Veg')
            
df['Type'] = meal_tags
df.insert(3, "Type", df.pop("Type"))

for i in range(len(df)):
    name_splitted = df.loc[i,'Name'].split()
    if ('Vegan' or 'vegan') in name_splitted:
        df.loc[i,'Type'] = 'Vegan'




df.rename(columns={"Ratings out of 5": "Ratings 0.0-5.0", "Type": "Preference"},inplace=True)
df['Ratings 0.0-5.0'] =  df['Ratings 0.0-5.0'].fillna(0)
df['Ratings count'] =  df['Ratings count'].fillna('0')



# CLeaning reviews, ratings, no of photos , cookng time since all of them has suffix after the numerical value 
#such as  (122 photos, 32 review, etc etc)

reviews_list = list(df['Reviews'].str.replace(r'[^\w\s]+', '',regex=True).values)

reviews_cleaned = []
for rev in reviews_list:
    if(pd.isna(rev)):
        reviews_cleaned.append(0)
    else:
        reviews_cleaned.append(int(rev.split(' ')[0]))
        
df['Reviews'] = reviews_cleaned



photos_list = list(df['Photos'].str.replace(r'[^\w\s]+', '',regex=True).values)

photos_cleaned = []
for pho in photos_list:
    if(pd.isna(pho)):
        photos_cleaned.append(0)
    else:
        photos_cleaned.append(int(pho.split(' ')[0]))
        
df['Photos'] = photos_cleaned




def get_cooking_time(cook_time):
    
    time = cook_time.split()
    if 'day' in time:
        index_d = time.index('day')
        n_day = time[index_d-1]
    else :
        n_day = 0
    
    if 'hrs' in time:
        index_h = time.index('hrs')
        n_hrs = time[index_h-1]
    else:
        n_hrs = 0
    
    if 'mins' in time:
        index_m = time.index('mins')
        n_mins = time[index_m-1]
    else:
        n_mins = 0
    
    time_in_mins = int(n_day) * 1440 +  int(n_hrs) * 60 +  int(n_mins)
    return time_in_mins



cooking_time_cleaned = []
for ct in df['Cooking time']:

    if(pd.isna(ct)):
        cooking_time_cleaned.append(np.nan)
    else:
        cooking_time_cleaned.append(get_cooking_time(ct))

df['Cooking time'] = cooking_time_cleaned



df['Cooking time'] = df['Cooking time'].fillna(0)
df=df.dropna(subset=['Calories']).reset_index(drop=True)
df=df.dropna(subset=['Servings']).reset_index(drop=True)

df['Fats'] = df['Fats'].fillna('0g')
df=df.dropna(subset=['Carbs']).reset_index(drop=True)

df['Proteins'] = df['Proteins'].fillna('0g')
df['Sugars'] = df['Sugars'].fillna('0g')


columns_to_proceed = ['Name','URL','Category','Preference','Description','Ratings 0.0-5.0','Ratings count','Reviews',
                     'Photos','Cooking time','Calories','Servings','Ingredients','no of steps',
                      'Fats','Carbs','Sugars','Proteins',]

df = df[columns_to_proceed]


df['Ratings count'] = pd.to_numeric(df['Ratings count'].str.replace(r'[^\w\s]+', '',regex=True))


df['Fats'] = pd.to_numeric(df['Fats'].str.replace('g',''))
df['Carbs'] = pd.to_numeric(df['Carbs'].str.replace('g',''))
df['Proteins'] = pd.to_numeric(df['Proteins'].str.replace('g',''))
df['Sugars'] = pd.to_numeric(df['Sugars'].str.replace('g',''))



df = df.rename(columns={"Ratings 0.0-5.0": "Ratings_0_5", "Ratings count": "Ratings_count",
                  "Cooking time": "Cooking_time","no of steps": "no_of_steps"})

df.to_csv('...data/processed/aws_allrecipes_data_cleaned.csv',index=False)

