from google.cloud import pubsub_v1
import pandas as pd
import os
import openpyxl

credentials_path = 'voltaic-genre-naturalDisaster.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/voltaic-genre-385510/topics/NaturalDiaster'


df = pd.read_excel('emdat_public.xlsx')
df = df.drop(index=range(5))
df = df.set_axis(df.iloc[0], axis=1).drop(df.index[0])

for index, row in df.iterrows():
    message_data = row.to_json().encode('utf-8')
    future = publisher.publish(topic_path, message_data)
    print(f'published message id {future.result()}')

