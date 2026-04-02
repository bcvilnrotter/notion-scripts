import requests

def convert_to_url(text):
    text = f'%22{text.strip()}%22'
    changes = {
        '&': '%26',
        ',': '%2C',
        '/': '%2F',
        '?': '%3F',
        ':': '%3A',
        '@': '%40',
        '#': '%23',
        '$': '%24',
        '+': '%2B',
        '=': '%3D',
        ' ': '%20',
    }
    return ''.join(changes.get(char, char) for char in text)

def convert_block_to_query(block):
    return '%20OR%20'.join(convert_to_url(name) for name in block)

def convert_block_to_comma(block):
    return ','.join(block)

def build_url_perigon(
        data,token,limit=1990, # save 10 characters for page parameter
        first='https://api.perigon.io/v1/companies/all?name=',stories=False):
    last_piece = f'&apiKey={token}'
    data = data.copy()

    while not all(data['used']):
        unused = data[data['used'] == False]
        names = []
        last_valid_url = None
        last_valid_indices = []

        for idx,row in unused.iterrows():
            names.append(row['name'])
            cbtq = convert_block_to_query
            cbtc = convert_block_to_comma
            query = cbtq(names) if not stories else cbtc(names)
            url = f'{first}{query}{last_piece}'

            if len(url) > limit:
                if last_valid_url is not None:
                    yield last_valid_url

                    data.loc[last_valid_indices,'used'] = True
                break

            last_valid_url = url
            last_valid_indices.append(idx)

        else:
            if last_valid_url:
                yield last_valid_url
                data.loc[last_valid_indices,'used'] = True

def pull_perigon_data(url,verbose=False):
    response = requests.get(url,headers={'Accept':'application/json'})
    response.raise_for_status()

    if verbose:
        print_string = f"  ... Found {response.json().get('numResults')} "
        print_string += "from Perigon link."
        print(print_string)
    
    page = 0
    full_results = response.json().get('numResults')
    cur_results = len(response.json().get('results'))

    while cur_results < full_results:
        for article in response.json().get('results'):
            yield article

        page += 1
        url += f'&page={page}' # TODO: it looks like page cannot be appended to end of url, need to find a work around.
        response = requests.get(url,headers={'Accept':'application/json'})
        response.raise_for_status()
        cur_results += len(response.json().get('results'))

def get_perigon_id_from_notion_page(page):
    try:
        return page.get(
            'properties').get(
                'Perigon.id').get(
                    'rich_text')[0].get('text').get('content')
    except:
        return ""