import requests, json, time, threading
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, ConnectionError, Timeout

session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    pool_block=True
)
session.mount('https://', adapter)

_rate_lock = threading.Lock()
_last_request_time = [0.0]
MIN_INTERVAL = 0.4

def _throttle():
    with _rate_lock:
        now = time.time()
        wait = MIN_INTERVAL - (now - _last_request_time[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_time[0] = time.time()

def upsert_entry_to_notion_database(
        headers,data,page_id) -> requests.Response:
    _throttle()
    response = None
    for attempt in range(5):
        try:
            if page_id != "empty":
                response = session.patch(
                    f'https://api.notion.com/v1/pages/{page_id}',
                    headers=headers, json=data.get('properties'),
                    timeout=30)            

            else:
                response = session.post(
                    'https://api.notion.com/v1/pages',
                    headers=headers, json=data,timeout=30)
            
            if response.status_code == 429:
                wait = float(
                    response.headers.get(
                        'Retry-After', 2 ** attempt))
                
                ret_aft = response.headers.get('Retry-After')
                note = f"[429] attempt {attempt}, waiting {wait}s"
                note += f", Retry-After={ret_aft}"
                print(ret_aft)
                
                time.sleep(wait)
                continue
            
            response.raise_for_status()
            return response
        
        except (SSLError, ConnectionError, Timeout) as e:
            print(f"[net retry {attempt}] {type(e).__name__}: {e}")
            time.sleep(2 ** attempt)
            continue
        
        except requests.HTTPError:
            if response is not None:
                err_msg = f"[HTTP error {response.status_code}]"
                err_msg += f" attempt {attempt}:"
                err_msg += f" {response.text[:200]}"
                print(err_msg)
            raise
    
    res = response.status_code if response else '?'
    rte_msg = "Notion upsert failed after 5 retries"
    rte_msg += f" (last status: {res})"
    raise RuntimeError(rte_msg)