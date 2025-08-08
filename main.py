#!/usr/bin/env python3
"""
Indeed CSV Downloader Actor - Python Version
Automates login to Indeed employer portal and downloads CSV reports
"""

import os
import time
import asyncio
import traceback
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

# Import Apify Actor
try:
    from apify import Actor
except ImportError:
    # Fallback for local development
    class Actor:
        def __init__(self):
            self.log = self
            self.input_data = {}
        
        def info(self, msg):
            print(f"INFO: {msg}")
        
        def warning(self, msg):
            print(f"WARNING: {msg}")
        
        def error(self, msg):
            print(f"ERROR: {msg}")
        
        def debug(self, msg):
            print(f"DEBUG: {msg}")
        
        async def get_input(self):
            return self.input_data
        
        async def push_data(self, data):
            print(f"PUSH DATA: {data}")
        
        async def set_value(self, key, value, content_type=None):
            print(f"SET VALUE: {key} = {value}")
        
        async def get_value(self, key):
            return None

# Configuration
CONFIG = {
    'COOKIES_KEY': 'indeed_cookies_v1',
    'KV_FILENAME_KEY': 'indeed_output_filename',
    'USERNAME_SELECTORS': [
        'input[type="email"]',
        'input[name="email"]',
        'input[name="username"]',
        'input[id*="email"]',
        'input[name="__email"]',
        'input[id*="login"]',
        '#signin-email'
    ],
    'PASSWORD_SELECTORS': [
        'input[type="password"]',
        'input[name="password"]',
        'input[id*="password"]',
        'input[name="__password"]',
        '#signin-password'
    ],
    'LOGIN_BUTTON_SELECTORS': [
        'button[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Sign In")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
        'input[type="submit"]',
        '.signin-button',
        '#signin-submit'
    ],
    'DOWNLOAD_BUTTON_TEXTS': [
        'Download CSV',
        'Export CSV',
        'Export',
        'Download',
        'Export candidates'
    ],
    'CANDIDATES_SELECTORS': [
        'a[href*="candidates"]',
        'a[href*="applicants"]',
        '.candidates-tab',
        '[data-testid="candidates"]',
        'text=Candidates',
        'text=Applicants'
    ]
}

async def try_fill(page: Page, selectors: list, value: str) -> bool:
    """Try to fill a field using multiple selectors"""
    for selector in selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                await page.fill(selector, value)
                return True
        except Exception:
            continue
    return False

async def try_click(page: Page, selectors: list) -> bool:
    """Try to click an element using multiple selectors"""
    for selector in selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                await page.click(selector)
                return True
        except Exception:
            continue
    return False

async def load_cookies_into_context(actor: Actor, context) -> bool:
    """Load cookies from Apify KV store"""
    try:
        cookies = await actor.get_value(CONFIG['COOKIES_KEY'])
        if cookies:
            await context.add_cookies(cookies)
            actor.log.info('Loaded cookies from KV store.')
            return True
    except Exception as e:
        actor.log.warning(f'Failed to load cookies: {e}')
    return False

async def save_cookies_from_context(actor: Actor, context) -> None:
    """Save cookies to Apify KV store"""
    try:
        cookies = await context.cookies()
        await actor.set_value(CONFIG['COOKIES_KEY'], cookies)
        actor.log.info('Saved cookies to KV store.')
    except Exception as e:
        actor.log.warning(f'Failed to save cookies: {e}')

async def post_file_to_webhook(actor: Actor, filepath: str, webhook_url: str) -> bool:
    """Post file to webhook URL"""
    if not webhook_url:
        actor.log.info('No webhook URL provided; skipping POST.')
        return False

    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            with open(filepath, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('file', f, filename=os.path.basename(filepath))
                
                async with session.post(webhook_url, data=data, timeout=60) as response:
                    if 200 <= response.status < 300:
                        actor.log.info(f'Successfully posted CSV to webhook: {webhook_url}')
                        return True
                    else:
                        actor.log.warning(f'Webhook POST returned status {response.status}')
                        return False
    except Exception as e:
        actor.log.error(f'Failed to POST file to webhook: {e}')
        return False

async def download_csv_via_click(page: Page, actor: Actor, config: Dict[str, Any]) -> Optional[str]:
    """Download CSV by clicking download buttons"""
    timeout = config.get('timeout', 30000)

    # Try clicking by text first
    for text in CONFIG['DOWNLOAD_BUTTON_TEXTS']:
        try:
            element = await page.query_selector(f'text="{text}"')
            if element:
                actor.log.info(f'Clicking download button by text: {text}')
                
                # Set up download listener
                download_info = None
                
                async def handle_download(download):
                    nonlocal download_info
                    download_info = download
                
                page.on('download', handle_download)
                
                await page.click(f'text="{text}"')
                
                # Wait for download
                await asyncio.sleep(5)
                
                if download_info:
                    out_path = f"/tmp/{config.get('download_filename', 'indeed-output.csv')}"
                    await download_info.save_as(out_path)
                    
                    if os.path.exists(out_path):
                        return out_path
        except Exception as e:
            actor.log.warning(f'Download timed out after clicking text "{text}": {e}')

    # Try generic selectors
    selector_candidates = [
        'a[download]',
        'a[href$=".csv"]',
        'button[data-test*="export"]',
        'button:has-text("Export")',
        'button:has-text("Download")',
        'a:has-text("Download CSV")',
        '.export-button',
        '.download-button',
        '[data-testid="export"]',
        '[data-testid="download"]'
    ]

    for selector in selector_candidates:
        try:
            element = await page.query_selector(selector)
            if element:
                actor.log.info(f'Clicking download element by selector: {selector}')
                
                download_info = None
                
                async def handle_download(download):
                    nonlocal download_info
                    download_info = download
                
                page.on('download', handle_download)
                
                await element.click()
                
                # Wait for download
                await asyncio.sleep(5)
                
                if download_info:
                    out_path = f"/tmp/{config.get('download_filename', 'indeed-output.csv')}"
                    await download_info.save_as(out_path)
                    
                    if os.path.exists(out_path):
                        return out_path
        except Exception as e:
            actor.log.warning(f'Download timed out after clicking selector "{selector}": {e}')

    return None

async def direct_download_by_url(page: Page, actor: Actor, config: Dict[str, Any]) -> Optional[str]:
    """Download CSV directly from URL"""
    try:
        csv_url = config.get('csv_download_url', '')
        if csv_url.lower().endswith('.csv'):
            actor.log.info('CSV URL looks direct; attempting direct GET')
            
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(csv_url, timeout=60) as response:
                    if response.status == 200:
                        content = await response.read()
                        out_path = f"/tmp/{config.get('download_filename', 'indeed-output.csv')}"
                        
                        with open(out_path, 'wb') as f:
                            f.write(content)
                        
                        if os.path.exists(out_path):
                            return out_path
    except Exception as e:
        actor.log.debug(f'Direct download attempt failed: {e}')

    return None

async def scan_for_csv_links(page: Page, actor: Actor, config: Dict[str, Any]) -> Optional[str]:
    """Scan page for CSV links and download"""
    try:
        links = await page.query_selector_all('a')
        for link in links:
            href = await link.get_attribute('href') or ''
            if href and '.csv' in href:
                actor.log.info(f'Found CSV link: {href} — attempting GET.')
                
                # Make absolute URL
                if href.startswith('http'):
                    csv_link = href
                else:
                    base_url = page.url.rstrip('/')
                    csv_link = f"{base_url}/{href.lstrip('/')}"
                
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(csv_link, timeout=60) as response:
                        if response.status == 200:
                            content = await response.read()
                            if len(content) > 50:  # Ensure it's not empty
                                out_path = f"/tmp/{config.get('download_filename', 'indeed-output.csv')}"
                                
                                with open(out_path, 'wb') as f:
                                    f.write(content)
                                
                                if os.path.exists(out_path):
                                    return out_path
    except Exception as e:
        actor.log.debug(f'Fallback link scan failed: {e}')

    return None

async def upload_to_kv_store(actor: Actor, file_path: str, filename: str) -> bool:
    """Upload file to Apify KV store"""
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        
        await actor.set_value(filename, content, content_type='text/csv')
        await actor.set_value(CONFIG['KV_FILENAME_KEY'], filename)
        actor.log.info(f'CSV uploaded to Apify KV store with key: {filename}')
        return True
    except Exception as e:
        actor.log.warning(f'Failed to upload CSV to KV store: {e}')
        return False

async def check_login_status(page: Page, actor: Actor) -> bool:
    """Check if already logged in"""
    try:
        download_element = await page.query_selector('text="Download"')
        export_element = await page.query_selector('text="Export"')
        export_csv_element = await page.query_selector('text="Export CSV"')

        if download_element or export_element or export_csv_element:
            actor.log.info('Detected download controls without fresh login — using existing cookies.')
            return True
        else:
            actor.log.debug('Download controls not detected on page (may require login).')
            return False
    except Exception:
        actor.log.debug('Login status check failed; will proceed with login flow.')
        return False

async def perform_login(page: Page, actor: Actor, config: Dict[str, Any]) -> bool:
    """Perform login to Indeed"""
    try:
        actor.log.info('Performing login flow.')
        await page.goto(config.get('login_url', 'https://employers.indeed.com/'), 
                       wait_until='networkidle', timeout=30000)

        # Fill email/username
        filled_user = await try_fill(page, CONFIG['USERNAME_SELECTORS'], config['username'])
        if not filled_user:
            actor.log.warning('Could not find username/email field using common selectors - trying generic input')
            try:
                await page.fill('input', config['username'])
            except Exception:
                actor.log.error('Unable to autofill username field; login may fail.')

        # Fill password
        filled_pass = await try_fill(page, CONFIG['PASSWORD_SELECTORS'], config['password'])
        if not filled_pass:
            actor.log.warning('Could not find password field using common selectors - trying generic input')
            try:
                inputs = await page.query_selector_all('input[type="password"]')
                if inputs:
                    await inputs[0].fill(config['password'])
            except Exception:
                actor.log.error('Unable to autofill password field; login may fail.')

        # Click login button
        clicked_login = await try_click(page, CONFIG['LOGIN_BUTTON_SELECTORS'])
        if not clicked_login:
            try:
                await page.click('text="Sign in"')
            except Exception:
                actor.log.warning('Could not click login button automatically; you may need to update selectors.')

        # Wait for navigation after login
        try:
            await page.wait_for_load_state('networkidle', timeout=20000)
        except Exception:
            actor.log.debug('Network idle wait timed out after login — continuing with checks.')

        return True
    except Exception as e:
        actor.log.error(f'Login failed: {e}')
        return False

async def download_csv(page: Page, actor: Actor, config: Dict[str, Any]) -> Optional[str]:
    """Download CSV using multiple methods"""
    # First attempt: direct .csv URL fetch
    if config.get('csv_download_url', '').lower().endswith('.csv'):
        result = await direct_download_by_url(page, actor, config)
        if result:
            return result

    # Second: try clicking download elements
    result = await download_csv_via_click(page, actor, config)
    if result:
        return result

    # Third: as fallback, try to find any link that points to .csv and GET it
    result = await scan_for_csv_links(page, actor, config)
    if result:
        return result

    return None

async def process_urls(page: Page, actor: Actor, config: Dict[str, Any]) -> Optional[str]:
    """Process all start URLs and attempt to download CSV from each"""
    start_urls = config.get('start_urls', [])
    
    for i, start_url in enumerate(start_urls):
        try:
            actor.log.info(f'Processing URL {i+1}/{len(start_urls)}: {start_url}')
            
            # Navigate to the URL
            await page.goto(start_url, wait_until='networkidle', timeout=30000)
            
            # Attempt CSV download
            out_path = await download_csv(page, actor, config)
            if out_path:
                return out_path
        except Exception as e:
            actor.log.warning(f'Failed to process URL {start_url}: {e}')
    
    return None

async def main_logic():
    """Main execution logic for the Indeed CSV Downloader Actor"""
    start_time = time.time()
    
    async with Actor:
        try:
            # Get input data
            input_data = await Actor.get_input() or {}
            
            # Create configuration
            config = {
                'start_urls': input_data.get('start_urls', ['https://employers.indeed.com/candidates']),
                'login_url': input_data.get('login_url', 'https://employers.indeed.com/'),
                'username': input_data.get('indeed_username', ''),
                'password': input_data.get('indeed_password', ''),
                'n8n_webhook_url': input_data.get('n8n_webhook_url', '').strip(),
                'save_cookies': input_data.get('save_cookies', True),
                'download_filename': input_data.get('download_filename', 'indeed-output.csv'),
                'max_retries': input_data.get('max_retries', 2),
                'timeout': input_data.get('timeout', 30000),
                'csv_type': input_data.get('csv_type', 'candidates'),
                'job_id': input_data.get('job_id', '')
            }
            
            # Basic validation
            if not config['username'] or not config['password']:
                Actor.log.error('Missing required inputs. Ensure indeed_username and indeed_password are provided.')
                await Actor.push_data({'status': 'Failed', 'error': 'Missing credentials'})
                return
            
            Actor.log.info('Starting Indeed CSV Downloader actor...')
            Actor.log.debug(f'Inputs: username={config["username"]} csv_type={config["csv_type"]} webhook_provided={"yes" if config["n8n_webhook_url"] else "no"}')
            
            # Provide retries for the whole flow
            for attempt in range(1, config['max_retries'] + 1):
                Actor.log.info(f'Flow attempt {attempt}/{config["max_retries"]}')
                
                try:
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        context = await browser.new_context()
                        
                        # Try to load cookies if present
                        try:
                            await load_cookies_into_context(Actor, context)
                        except Exception:
                            Actor.log.debug('No cookies loaded (or failed). Proceeding to login flow.')
                        
                        page = await context.new_page()
                        
                        # Check if already logged in
                        logged_in = False
                        try:
                            # Try visiting the first start_url to check login status
                            if config['start_urls']:
                                await page.goto(config['start_urls'][0], wait_until='networkidle', timeout=30000)
                                logged_in = await check_login_status(page, Actor)
                        except Exception:
                            Actor.log.debug('Visiting first start_url to check cookies failed/redirected; will open login page.')
                        
                        # Perform login if needed
                        if not logged_in:
                            if not await perform_login(page, Actor, config):
                                raise RuntimeError('Login failed')
                            
                            # Save cookies if enabled
                            if config['save_cookies']:
                                await save_cookies_from_context(Actor, context)
                        
                        # Process all start URLs and attempt download
                        out_path = await process_urls(page, Actor, config)
                        if not out_path:
                            raise RuntimeError('Unable to find or download CSV from any provided URL. Please verify start_urls and selectors.')
                        
                        Actor.log.info(f'CSV downloaded to {out_path}')
                        
                        # Upload to Apify KV store
                        kv_upload_success = await upload_to_kv_store(Actor, out_path, config['download_filename'])
                        
                        # Post to n8n webhook if provided
                        webhook_sent = False
                        if config['n8n_webhook_url']:
                            webhook_sent = await post_file_to_webhook(Actor, out_path, config['n8n_webhook_url'])
                        
                        # Close browser
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        
                        # Calculate execution time
                        execution_time = time.time() - start_time

                        # Create output
                        output = {
                            'status': 'Success',
                            'csv_type': config['csv_type'],
                            'csv_filename': config['download_filename'],
                            'file_size': os.path.getsize(out_path) if out_path and os.path.exists(out_path) else None,
                            'download_method': 'multiple_attempts',
                            'execution_time': execution_time,
                            'cookies_saved': config['save_cookies'],
                            'webhook_sent': webhook_sent,
                            'job_id': config['job_id']
                        }
                        
                        # Push output to dataset
                        await Actor.push_data(output)
                        
                        Actor.log.info('✅ CSV download flow completed successfully.')
                        return
                        
                except Exception as e:
                    Actor.log.error(f'Flow attempt {attempt} failed: {e}')
                    Actor.log.debug(traceback.format_exc())
                    
                    if attempt < config['max_retries']:
                        wait = 5 * attempt
                        Actor.log.info(f'Retrying in {wait}s...')
                        await asyncio.sleep(wait)
                    else:
                        Actor.log.critical('All attempts failed. Exiting with error.')
                        
                        # Create error output
                        output = {
                            'status': 'Failed',
                            'error': str(e),
                            'execution_time': time.time() - start_time
                        }
                        await Actor.push_data(output)
                        raise
                        
        except Exception as e:
            Actor.log.critical(f'Critical error in main logic: {e}')
            Actor.log.debug(traceback.format_exc())
            raise

if __name__ == "__main__":
    asyncio.run(main_logic())
