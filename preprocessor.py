import pandas as pd
import re

# Function to clean text content
def clean_text(text):
    text = re.sub(r'[\r\n]+', ' ', text)  # flatten multiline reviews
    text = text.lower()  # convert to lowercase
    text = re.sub(r'[^a-z\s]', '', text)  # remove non-alphabetical characters
    return text

df = pd.read_csv("steam_reviews.csv") # load the CSV

df = df[df['language'] == 'english'] # only select English reviews

df = df[df['review'].notnull() & (df['review'].str.strip() != '')] # remove null and empty reviews

df['review'] = df['review'].apply(clean_text) # clean the review text using the function

df = df[['app_name', 'review']] # remove unnecessary columns

df.to_csv("cleaned_reviews.tsv", sep="\t", index=False) # save output as .tsv 