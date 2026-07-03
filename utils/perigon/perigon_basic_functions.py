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
    """
    Fetch results for a Perigon URL. Uses requests.get(base, params=...)
    and yields individual result items. If a Perigon request returns a
    4xx error, log full response and stop yielding for that URL (do not raise).
    """
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    # parse_qs -> lists; convert to single values where appropriate
    raw_qs = parse_qs(parsed.query)
    params = {k: (','.join(v) if len(v) > 1 else v[0]) for k, v in raw_qs.items()}

    try:
        response = requests.get(base, headers={'Accept': 'application/json'}, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Log details and stop iterating this URL instead of raising
        print(f"Perigon request failed: status={getattr(response,'status_code',None)} url={getattr(response,'url',url)}")
        print("Response body:", getattr(response, 'text', '<no body>'))
        return
    except requests.exceptions.RequestException as e:
        # Network/timeout/etc — log and stop iterating this URL
        print(f"Perigon request error for url={url}: {e}")
        return

    if verbose:
        print(f"  ... Found {response.json().get('numResults')} items from Perigon link.")

    page = int(params.get('page', 0))
    full_results = response.json().get('numResults', 0)
    cur_results = len(response.json().get('results', []))

    # yield items from first page
    for item in response.json().get('results', []):
        yield item

    token_used = 1
    # if there are more pages, fetch them by updating params['page']
    while cur_results < full_results:
        page += 1
        params['page'] = page
        print(f"  ... Pulling page {page} of Perigon results.")
        try:
            response = requests.get(base, headers={'Accept': 'application/json'}, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print(f"Perigon request failed on page {page}: status={getattr(response,'status_code',None)} url={getattr(response,'url')}")
            print("Response body:", getattr(response, 'text', '<no body>'))
            return
        except requests.exceptions.RequestException as e:
            print(f"Perigon request error while fetching page {page} for url={base} params={params}: {e}")
            return

        results = response.json().get('results', [])
        for item in results:
            yield item

        cur_results += len(results)
        token_used += 1
        print(f"  ... Pulled {cur_results} of {full_results} total items.")

    print(f"   > Used {token_used} API token{'s' if token_used > 1 else ''}.")

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
