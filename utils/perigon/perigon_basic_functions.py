import requests,re

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
        data,token,limit=2000,
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
    token_used = 1

    if verbose:
        print_string = f"  ... Found {response.json().get('numResults')}"
        print_string += " items from Perigon link."
        print(print_string)
    
    page = 0
    full_results = response.json().get('numResults')
    cur_results = len(response.json().get('results'))

    if cur_results < full_results:
        while cur_results < full_results:
            print(f"  ... Pulling page {page} of Perigon results.")
            for item in response.json().get('results'):
                yield item

            page += 1
            url = re.sub(r'&page=\d+', f'&page={page}', url)
            response = requests.get(url,headers={'Accept':'application/json'})
            response.raise_for_status()
            cur_results += len(response.json().get('results'))
            print(f"  ... Pulled {cur_results} of {full_results} total items.")
            token_used += 1
    else:
        for item in response.json().get('results'):
            yield item
    
    token_used_ps = f"   > Used {token_used}"
    token_used_ps += f" API token{'s' if token_used > 1 else ''}."
    print(token_used_ps)

def get_perigon_id_from_notion_page(page):
    try:
        return page.get(
            'properties').get(
                'Perigon.id').get(
                    'rich_text')[0].get('text').get('content')
    except:
        return ""

def find_page_by_perigon_id(dbid,perigon_id,headers):
    query = {
        "filter": {
            "property": "Perigon.id",
            "rich_text": {
                "equals": perigon_id.strip()
            }
        }
    }
    response = requests.post(
        f'https://api.notion.com/v1/databases/{dbid}/query',
        headers=headers,json=query)
    response.raise_for_status()
    results = response.json().get('results')
    return results[0] if results else None