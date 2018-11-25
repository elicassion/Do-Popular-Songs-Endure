import os

import google.oauth2.credentials

import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

import pandas as pd
import codecs, os
# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "client_secret.json"

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

KEY = 'AIzaSyBv7xjl2DK7Wk6omOS6ZCehIq8euxNajms'

def get_authenticated_service():
  # flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
  # credentials = flow.run_console()
  return build(API_SERVICE_NAME, API_VERSION, developerKey='AIzaSyBv7xjl2DK7Wk6omOS6ZCehIq8euxNajms')

def print_response(response):
  print(response)

# Build a resource based on a list of properties given as key-value pairs.
# Leave properties with empty values out of the inserted resource.
def build_resource(properties):
  resource = {}
  for p in properties:
    # Given a key like "snippet.title", split into "snippet" and "title", where
    # "snippet" will be an object and "title" will be a property in that object.
    prop_array = p.split('.')
    ref = resource
    for pa in range(0, len(prop_array)):
      is_array = False
      key = prop_array[pa]

      # For properties that have array values, convert a name like
      # "snippet.tags[]" to snippet.tags, and set a flag to handle
      # the value as an array.
      if key[-2:] == '[]':
        key = key[0:len(key)-2:]
        is_array = True

      if pa == (len(prop_array) - 1):
        # Leave properties without values out of inserted resource.
        if properties[p]:
          if is_array:
            ref[key] = properties[p].split(',')
          else:
            ref[key] = properties[p]
      elif key not in ref:
        # For example, the property is "snippet.title", but the resource does
        # not yet have a "snippet" object. Create the snippet object here.
        # Setting "ref = ref[key]" means that in the next time through the
        # "for pa in range ..." loop, we will be setting a property in the
        # resource's "snippet" object.
        ref[key] = {}
        ref = ref[key]
      else:
        # For example, the property is "snippet.description", and the resource
        # already has a "snippet" object.
        ref = ref[key]
  return resource

# Remove keyword arguments that are not set
def remove_empty_kwargs(**kwargs):
  good_kwargs = {}
  if kwargs is not None:
    for key, value in kwargs.items():
      if value:
        good_kwargs[key] = value
  return good_kwargs

def search_list_by_keyword(client, **kwargs):
  # See full sample for function
  kwargs = remove_empty_kwargs(**kwargs)

  response = client.search().list(
    **kwargs
  ).execute()

  return response

def videos_list_by_id(client, **kwargs):
  # See full sample for function
  kwargs = remove_empty_kwargs(**kwargs)

  response = client.videos().list(
    **kwargs
  ).execute()

  return response

def store_checkpoint(d, counter, max_counter):
  file_name = '..\\data\\raw\\count_youtube.csv'
  df = pd.DataFrame.from_dict(d)
  print ("####### Processed {}/{} #######".format(format(counter, ','), format(1000000, ',')))
  if os.path.exists(file_name):
    df.to_csv(file_name, mode='a', header=False, index=False)
  else:
    f = codecs.open(file_name, 'w')
    f.close()
    df.to_csv(file_name, mode='a', index=False)
  # if os.path.exists(checkpoint_filename):
  cf = codecs.open(checkpoint_filename, 'w')
  cf.write(str(counter))
  cf.close()

def make_data_dict(counter, last_counter, 
  titles, artists, view_ct, like_ct, dislike_ct, comment_ct):

  d = {
    'title': titles[last_counter:counter],
    'artist_name': artists[last_counter:counter],
    'view_count': view_ct,
    'like_count': like_ct,
    'dislike_count': dislike_ct,
    'comment_count': comment_ct
  }
  return d

if __name__ == '__main__':
  # When running locally, disable OAuthlib's HTTPs verification. When
  # running in production *do not* leave this option enabled.
  os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '0'
  client = get_authenticated_service()
  
  msd = pd.read_csv('..\\data\\extracted\\MSDSet.csv')
  titles = msd['title'].values.tolist()
  artists = msd['artist_name'].values.tolist()
  del msd

  view_ct = []
  like_ct = []
  dislike_ct = []
  comment_ct = []

  checkpoint_counter = 0
  checkpoint_filename = 'count_youtube_checkpoint'
  if os.path.exists(checkpoint_filename):
    cf = open(checkpoint_filename, 'r')
    checkpoint_counter = int(cf.read().strip())
    cf.close()
  titles = titles[checkpoint_counter:]
  artists = artists[checkpoint_counter:]
  last_counter = 0
  max_counter = len(titles)
  counter = 0
  try:
    for t, a in zip(titles, artists):
      sres = search_list_by_keyword(client,
        part='snippet',
        maxResults=1,
        q=' '.join([t.strip().lower(), a.strip().lower()]), 
        type='video')
      if len(sres['items']) < 1:
        view_ct.append(-1)
        like_ct.append(-1)
        dislike_ct.append(-1)
        comment_ct.append(-1)
      else:
        vid = sres['items'][0]['id']['videoId']
        del sres

        vres = videos_list_by_id(client,
          part='statistics',
          id=vid)
        st = vres['items'][0]['statistics']
        view_ct.append(st['viewCount'])

        if 'likeCount' in st:
          like_ct.append(st['likeCount'])
        else:
          like_ct.append(-1)
        
        if 'dislikeCount' in st:
          dislike_ct.append(st['dislikeCount'])
        else:
          dislike_ct.append(-1)
        
        if 'commentCount' in st:
          comment_ct.append(st['commentCount'])
        else:
          comment_ct.append(-1)
      
      counter += 1
      # print (counter)
      if counter % 1000 == 0:
        d = make_data_dict(counter, last_counter, 
          titles, artists, view_ct, like_ct, dislike_ct, comment_ct)
        # print (d)
        store_checkpoint(d, checkpoint_counter + counter, max_counter)
        last_counter = counter
        view_ct = []
        like_ct = []
        dislike_ct = []
        comment_ct = []

  except Exception as e:
    print (e)
    l = len(view_ct)
    d = make_data_dict(counter, last_counter, 
      titles, artists, view_ct, like_ct, dislike_ct, comment_ct)
    store_checkpoint(d, checkpoint_counter + counter, max_counter)
    last_counter = counter
    view_ct = []
    like_ct = []
    dislike_ct = []
    comment_ct = []