import time
import re
import requests as r
from collections import OrderedDict
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

def format_description(row):
    # Format the description and insert the image tag for the html output.
    result = fr"""##########################<br>{row.name}<br>n##########################<br><img src="images/{row.name}.jpg"><br>{row.description}<br>"""
    return result

site_root = 'https://radioetiopia.podomatic.com/'

# There are a total of 82 pages on the site.
num_pages = 82

# Generate the URL for each page.
page_links = [site_root+F'?p={ii}' for ii in range(1,83)]

# For testing, just use the first page. Comment out the line below to scrape all pages.
page_links = page_links[0]

# Loop through the pages to get the URLs of each episode, using regex to extract the Permalinks.
# Save them to a dict with the key being the page number.
episode_links = {}
for one_page in page_links:
    response = r.get(one_page)
    assert response.status_code==200, 'Invalid response.'

    # # Using bs4 to extract the links.
    # soup = BeautifulSoup(response.content)
    # tt = soup.find_all('a', text='Permalink')
    # links = [ item.decode().replace('<a href="', '').replace('">Permalink</a>', '') for item in tt ]

    # Using regex.
    # The pattern, https?, works for both secure and non-secure connections.
    pattern = re.compile('href="(https?://[^\s]+)">Permalink</a>')
    links = re.findall(pattern, str(response.content))

    key = f"p{int(one_page.split('=')[1]):02d}"
    episode_links[key] = links
    print(F'Finished ...{key}')
    time.sleep(15)


# Process the page source of each episode page to get the description.
all_episodes = {}
for key, value in episode_links.items():
    for one_episode in value:
        episode_name = one_episode.split('episodes/')[1]
        response = r.get(one_episode)
        assert response.status_code==200, 'Invalid response.'

        soup = BeautifulSoup(response.content)
        tt = soup.find('meta', attrs={'name':'description'})
        description = tt.decode().replace('<meta content="', '').replace('" name="description"/>', '')

        all_episodes[episode_name] = description

        print(F'Processing... {episode_name}')
        time.sleep(5)

# Convert the data into dataframes.
links_df = pd.DataFrame.from_dict(episode_links, orient='index')

episode_df = pd.DataFrame.from_dict(all_episodes, orient='index', columns=['description'])

# Download the mp3s.

# For recent episodes, when it is available, download the file from podomatic.
failure = []
success = []
for index, _ in episode_df.loc[:'2017-05-25T12_00_00-07_00'].iterrows():
    url = f"https://radioetiopia.podomatic.com/enclosure/{index}.mp3"
    print(f"Downloading...{index}.mp3")
    response = r.get(url)
    if response.status_code==200:
        print(response)
        with open(f'{index}.mp3', 'wb') as f:
             f.write(response.content)
        success.append(index)
    else:
        failure.append(index)
        print('failed!')
    time.sleep(20)

# For older episodes, parse out the filefactory links and see if they are still downloadable.
pattern = re.compile('(https?://www.filefactory.com/file/[^\s]+mp3)')
episode_df['filefactory_link'] = episode_df['description'].str.extract(pattern)

not_downloaded = []
# Going from the oldest to the newest episodes.
for indx, (_, ffactory_link, _) in episode_df.loc['2017-05-18T13_00_00-07_00':'2011-07-03T12_42_57-07_00'].iloc[::-1].iterrows():
    if pd.notnull(ffactory_link):
        response = r.get(ffactory_link)
        if response.content[:9] != b'<!DOCTYPE':
            with open(indx+'.mp3', 'wb') as f:
                f.write(response.content)
            print(f"{indx}: Downloaded...{indx}.mp3")
            time.sleep(5)
        else:
            print(f"{indx}: The filefactory link is no longer valid.")
            not_downloaded.append(indx)
            time.sleep(3)
    else:
        print(f"{indx}: No filefactory link aviable.")
        not_downloaded.append(indx)
        time.sleep(1)


# Parse out the image links for each episode and save them.
image_links = {}
for one_page in page_links:
    response = r.get(one_page)
    assert response.status_code==200, 'Invalid response.'

    # # Using bs4 to extract the links.
    # soup = BeautifulSoup(response.content)
    # jj = soup.find_all(alt="itunes pic")
    # links = [ str(item).split('isrc="')[1].split('" src="')[0] for item in jj]

    # Using regex.
    # The pattern, https?, works for both secure and non-secure connections.
    pattern = re.compile('isrc="(https?://[^\s]+)" src=')
    links = re.findall(pattern, str(response.content))
    pattern = re.compile('href="(https?://[^\s]+)">Permalink</a>')
    keys = re.findall(pattern, str(response.content))
    keys = [ key.split('episodes/')[1] for key in keys]
    for one_key, one_link in zip(keys, links):
        image_links[one_key] = one_link
    print(F'Finished ...{one_page}')
    time.sleep(2)


# Convert the dict into a DataFrame.
image_df = pd.DataFrame.from_dict(image_links, orient='index', columns=['image_link'])

# Download the images.
for indx, item in image_df.iterrows():
    response = r.get(item['image_link'])
    if response.content[:9] != b'<!DOCTYPE':
        with open(f'images/{indx}.jpg', 'wb') as f:
            f.write(response.content)
        print(f"{indx}: Downloaded...image for {indx}")
        time.sleep(1)
    else:
        print(f"{indx}: The image link is invalid.")
        time.sleep(1)

# Add an HTML linebreak tag, <br>, to the end of each line in the description strings.
episode_df['description'] = episode_df['description'].str.replace(r'\r\n', r'<br>\r\n')

# Save all the description into one big hmtl file, separated by the episode timestamp.
# Now also embed the images in the html file.
episode_df['formatted_description'] = episode_df.apply(format_description, axis=1)
output = '\n'.join(episode_df.formatted_description.values)
with open('radio_etiopia_episode_descriptions_v2.html', 'w', encoding='utf-8') as f:
    f.write(output)

# I have tried to use VIM to add a linebreak tag, <br>, to the beginning of each line in the file.

# Save the DataFrames in a HDF file.
datafile = 'page_and_episode_info.h5'
links_df.to_hdf(datafile, key='page_links', mode='w')
episode_df.to_hdf(datafile, key='episode_info')
image_df.to_hdf(datafile, key='image_links')
