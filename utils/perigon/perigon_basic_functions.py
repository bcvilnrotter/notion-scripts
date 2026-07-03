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

def pull_perigon_data(url, verbose=False):
    def _get(u):
        r = requests.get(u, headers={'Accept': 'application/json'})
        if not r.ok:
            # surface Perigon's actual reason instead of a bare 400
            print(f"    ! Perigon {r.status_code} for {u.split('&apiKey=')[0]}")
            print(f"    ! body: {r.text[:500]}")
        r.raise_for_status()
        return r.json()

    data = _get(url)
    token_used = 1
    full_results = data.get('numResults')
    if verbose:
        print(f" ... Found {full_results} items from Perigon link.")

    page = 0
    pulled = 0
    size = 100  # matches &size=100 in url_base

    while True:
        results = data.get('results') or []
        print(f" ... Pulling page {page} of Perigon results.")
        for item in results:
            yield item
        pulled += len(results)
        print(f" ... Pulled {pulled} of {full_results} total items.")

        # stop conditions: short/empty page = we've hit the real end
        if len(results) < size or (full_results and pulled >= full_results):
            break

        page += 1
        next_url = re.sub(r'&page=\d+', f'&page={page}', url)
        try:
            data = _get(next_url)
            token_used += 1
        except requests.exceptions.HTTPError as e:
            # Perigon 400s an out-of-range page instead of returning []
            if e.response is not None and e.response.status_code == 400:
                print(f" ... Reached Perigon's last available page at page {page}. Stopping.")
                break
            raise

    print(f" > Used {token_used} API token{'s' if token_used > 1 else ''}.")

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
