"""TopCV Selenium transport for the *new* crawler, without Raw/DB side effects.

This module supports a safe manual-search inspection of TopCV followed by
programmatic listing/detail capture. Automatic search/filter selectors cannot
be validated using the stored listing/detail HTML fixtures, therefore the
unverified automatic path fails closed until search UI DOM is calibrated.

No crawl-run orchestration, scheduling, or SQL writes live here.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import time
import unicodedata
from typing import Mapping
from urllib.parse import urlsplit

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from . import selectors as S
from .config import CrawlerConfig
from .parser import (
    AccessChallenge, LayoutChanged, check_challenge, normalize_job_link,
    parse_detail_page, parse_listing_page,
)


# Probe only the *verified* TopCV listing/empty selectors. Neither a URL
# change nor document.readyState=complete proves AJAX results have arrived.
# Return bounded structural diagnostics, never full HTML or input values.
_LISTING_STATE_JS = r"""
const root = document.querySelector('.job-list-search-result');
const cards = root ? Array.from(root.querySelectorAll(':scope > .job-item-search-result')) : [];
const empty = document.querySelector('.none-suitable-job__content');
const visible = (el) => {
 if (!el) return false;
 const cs = getComputedStyle(el), r = el.getBoundingClientRect();
 return cs.display !== 'none' && cs.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};
const containerPattern = /job-list|search-result|none-suitable|no-result|no-job|empty-result|loading|spinner|skeleton/i;
const selected = Array.from(document.querySelectorAll('#main [class], #main [id]'))
 .filter(el => visible(el) && containerPattern.test(String(el.className || '')+' '+(el.id || '')))
 .slice(0, 16).map(el => ({
    tag: el.tagName.toLowerCase(),
    class: String(el.className || '').slice(0, 110),
    id: String(el.id || '').slice(0, 80)
 }));
const main = document.querySelector('#main');
const topSections = main ? Array.from(main.children).slice(0, 14).map(el => ({
    tag: el.tagName.toLowerCase(),
    id: String(el.id || '').slice(0, 80),
    class: String(el.className || '').slice(0, 110),
    visible: visible(el)
})) : [];
const jobIds = cards.slice(0, 8).map(el => String(el.getAttribute('data-job-id') || '').slice(0, 24));
const emptyVisible = visible(empty);
const domReady = Boolean(cards.length || emptyVisible);
return {
  document_ready_state: document.readyState,
  listing_container_present: Boolean(root),
  listing_cards_count: cards.length,
  empty_marker_present: Boolean(empty),
  empty_marker_visible: emptyVisible,
  verified_structure_present: domReady,
  signature: (cards.length ? 'cards:'+cards.length+':'+jobIds.join(',') : (emptyVisible ? 'empty' : 'none')),
  sample_job_ids: jobIds,
  relevant_visible_containers: selected,
  main_top_sections: topSections
};
"""


class NavigationError(RuntimeError):
    """A page cannot be reliably navigated/verified; do not save it as success."""


# Navigation Timing records the main document's HTTP status (Chrome 109+).
_DOCUMENT_HTTP_STATUS_JS = (
    "const entry = performance.getEntriesByType('navigation')[0];"
    "return entry && typeof entry.responseStatus === 'number' ? entry.responseStatus : null;"
)


@dataclass(frozen=True)
class ListingCapture:
    page_url: str
    page_html: str
    items: list[dict]
    captured_at: datetime
    rejected_cards: tuple[dict, ...] = ()
    http_status: int | None = None


@dataclass(frozen=True)
class DetailCapture:
    job_url: str
    detail_html: str
    parsed: dict
    captured_at: datetime
    http_status: int | None = None


# Read-only UI inventory. Never reads input values, cookies, localStorage, or
# HTML of profile/account panels. This is a DOM calibration aid, NOT verified
# selectors for automatic searching.
_NAVIGATION_AUDIT_JS = r"""
// Read-only, mode-aware UI survey. No cookie/storage access and NO input values.
const limit = Math.max(20, Math.min(Number(arguments[0]) || 120, 240));
const stage = String(arguments[1] || 'entry');
const targetKeyword = String(arguments[2] || '');
const safe = (v,n=100)=>String(v||'')
  .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,'[email]')
  .replace(/(?:\+84|0)(?:[ .-]?[0-9]){9,10}/g,'[phone]')
  .replace(/\s+/g,' ').trim().slice(0,n);
const visible=(el)=>{
 if(!el) return false;
 const r=el.getBoundingClientRect(), st=window.getComputedStyle(el);
 return st.visibility!=='hidden' && st.display!=='none' && r.width>0 && r.height>0;
};
const label = (selector)=>{
  const el=document.querySelector(selector);
  return visible(el) ? safe(el.innerText,90) : null;
};
const cssPath=(el)=>{
 const a=[];
 for(let i=0;el&&el.nodeType===1&&i<5;el=el.parentElement,i++){
   let s=el.tagName.toLowerCase();
   if(el.id && /^[a-zA-Z][\w:-]{0,70}$/.test(el.id)) { a.unshift(s+'#'+CSS.escape(el.id)); break; }
   const cl=Array.from(el.classList||[]).filter(c=>/^[a-zA-Z][\w-]{0,55}$/.test(c)).slice(0,2);
   if(cl.length) s+='.'+cl.map(c=>CSS.escape(c)).join('.');
   if(el.parentElement) {
      const siblings=Array.from(el.parentElement.children).filter(c=>c.tagName===el.tagName);
      if(siblings.length>1) s+=`:nth-of-type(${siblings.indexOf(el)+1})`;
   }
   a.unshift(s);
 }
 return a.join(' > ').slice(0,320);
};
const hint=/search|filter|category|ngành|nghề|vị trí|keyword|location|city|tỉnh|quận|huyện|phường|địa điểm|hà nội|hồ chí minh|công nghệ|chọn|xác nhận|tiếp|next|page|pagination|option|dropdown|popup|dialog|select|list|apply|áp dụng/i;
const selector = [
  '#frm-search-job *','#box-option-search-job *',
  '[role="dialog"] *','[role="listbox"] *','[role="option"]',
  '[aria-modal="true"] *',
  '[class*="category"]','[class*="Category"]','[class*="location"]',
  '[class*="Location"]','[class*="province"]','[class*="district"]',
  '[class*="dropdown"]','[class*="Dropdown"]','[class*="popup"]',
  '[class*="Popup"]','[class*="option"]','[class*="Option"]',
  '[class*="pagination"]','[class*="paging"]','[class*="city"]',
  'button','input','select','label','[role="combobox"]','[role="button"]',
  '[aria-expanded]','[aria-selected]'
].join(',');
const nodes=Array.from(document.querySelectorAll(selector));
const found=[];
for(const el of nodes) {
 if(!visible(el)) continue;
 const tag=el.tagName.toLowerCase();
 const role=el.getAttribute('role')||'';
 const attrs={};
 for(const k of ['id','name','type','role','placeholder','aria-label',
                  'aria-expanded','aria-selected','aria-controls','data-testid','title']) {
   if(el.hasAttribute(k)) attrs[k]=safe(el.getAttribute(k),90);
 }
 attrs.class=safe(typeof el.className==='string'?el.className:'',140);
 const text=safe(el.innerText||(tag==='label'?el.textContent:''),100);
 const parent=el.parentElement;
 const parent_class=safe(parent && typeof parent.className==='string' ? parent.className:'',100);
 const ctx=[text,...Object.values(attrs),parent_class].join(' ');
 const isInput=['input','select','button'].includes(tag)||['option','combobox','button'].includes(role);
 if(!hint.test(ctx) && !isInput) continue;
 const withinForm=Boolean(el.closest('#frm-search-job'));
 const isCategory=/category|ngành|nghề|công nghệ thông tin/i.test(ctx);
 const isLocation=/location|province|city|tỉnh|quận|huyện|phường|địa điểm|hà nội/i.test(ctx);
 const isPaging=/pagination|paging|next|trang sau|trang tiếp|page-number/i.test(ctx);
 const inOverlay=Boolean(el.closest('[role="dialog"],[role="listbox"],[aria-modal="true"],'+
   '[class*="dropdown"],[class*="popup"],[class*="Popup"]'));
 // Favor scoped form controls and transient picker options rather than 1000+ cards.
 let score=(withinForm?100:0)+(inOverlay?125:0)+(isInput?30:0);
 if(stage==='sector_picker_open') score+=isCategory?240:0;
 if(stage==='location_picker_open') score+=isLocation?240:0;
 if(stage==='search_results') score+=isPaging?200:0;
 if(stage==='entry') score+=withinForm?140:0;
 if(role==='option'||tag==='label') score+=50;
 if(el.id==='keyword') score+=350;
 if(el.id==='category-filter-desktop-container-box-search-page-job'||
    el.id==='location-selection-desktop-job-listing-box-search-job') score+=350;
 if(!withinForm && !inOverlay && /job-item-search-result|job-list-search-result|box-job-list/.test(cssPath(el))) score-=350;
 found.push({score, data:{tag,css_path:cssPath(el),text,attributes:attrs,parent_class}});
}
found.sort((a,b)=>b.score-a.score);
const kw=document.querySelector('#frm-search-job #keyword');
const selectedCategory=label('#category-filter-desktop-container-box-search-page-job .select-category');
const selectedLocation=label('#location-selection-desktop-job-listing-box-search-job .select-multi-location');
return {
 page_url: location.origin + location.pathname,
 query_parameter_keys: Array.from(new URLSearchParams(location.search).keys()).slice(0,16),
 page_title: safe(document.title,130),
 total_visible_relevant_controls: found.length,
 truncated: found.length > limit,
 controls: found.slice(0,limit).map(x=>x.data),
 observed_state: {
   search_form_present: Boolean(document.querySelector('#frm-search-job')),
   keyword_control_present: Boolean(kw),
   keyword_matches_target: kw ? String(kw.value||'').trim().toLocaleLowerCase() === targetKeyword.trim().toLocaleLowerCase() : false,
   category_label: selectedCategory,
   location_label: selectedLocation,
   heading: label('#search-job-heading'),
   listing_cards_in_dom: document.querySelectorAll('.job-list-search-result > .job-item-search-result').length,
   visible_dialog_count: Array.from(document.querySelectorAll('[role="dialog"],[aria-modal="true"]')).filter(visible).length,
   location_notice_open: Boolean(document.querySelector('#location-selection__notice-new-location.open'))
 }
};
"""


class TopCVCrawler:
    def __init__(self, cfg: CrawlerConfig, artifacts_dir: str | Path = 'artifacts', *, observe_http=False):
        if not isinstance(cfg, CrawlerConfig):
            raise TypeError('TopCVCrawler expects load_runtime_config(), not a dict.')
        self.cfg = cfg
        self.driver = None
        self.artifacts = Path(artifacts_dir)
        if not self.artifacts.is_absolute():
            self.artifacts = cfg.config_path.parent.parent / self.artifacts
        self.wait_seconds = cfg.selenium.explicit_wait_seconds
        self.observe_http = observe_http
        self.last_response_evidence = {}
        self._last_http_error = None
        self._http_checked = False
        self._on_wait_tick = None
        # Set only by the opt-in undetected transport (src/crawler/undetected.py).
        self._uc = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.close()

    def start(self):
        if self.driver is not None:
            raise RuntimeError('Chrome is already started.')
        if self.cfg.selenium.browser != 'chrome':
            raise ValueError('This Selenium transport supports Chrome only.')
        if self.cfg.selenium.driver == 'undetected':
            from .undetected import UndetectedSession
            self._uc = UndetectedSession.start(
                self.cfg.selenium, in_container=os.environ.get('TOPCV_CONTAINER_BROWSER') == '1')
            self.driver = self._uc.driver
            return self
        options = self.cfg.selenium.build_chrome_options()
        if self.observe_http:
            options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        # Chrome creates its own profile folder; no profile is shipped with ZIP.
        driver_path = os.environ.get('CHROMEDRIVER_PATH') or None
        if driver_path:
            from selenium.webdriver.chrome.service import Service
            self.driver = webdriver.Chrome(service=Service(executable_path=driver_path), options=options)
        else:
            self.driver = webdriver.Chrome(options=options)
        try:
            self.driver.set_page_load_timeout(self.cfg.selenium.page_load_timeout_seconds)
            return self
        except Exception:
            self.close()
            raise

    def close(self):
        driver, self.driver = self.driver, None
        self._uc = None
        if driver is not None:
            driver.quit()

    def _driver(self):
        if self.driver is None:
            raise RuntimeError('Browser not started. Use TopCVCrawler(...).start().')
        return self.driver

    def _goto(self, url: str) -> None:
        """Navigate the current tab; the undetected transport loads the page detached."""
        if self._uc is None:
            self._driver().get(url)
        else:
            self._uc.navigate(url, self._on_wait_tick)

    def _verification_sleep(self, seconds: float, remaining: float) -> None:
        if self._uc is None:
            time.sleep(seconds)
            return
        # A person is completing the check: leave Chrome with no driver attached
        # and only attach briefly, a few times a minute, to see whether it is done.
        self._uc.idle(min(max(seconds, 8.0), remaining))

    def _wait_out_challenge(self, exc: AccessChallenge) -> bool:
        """Give the browser's own non-interactive check time to finish by itself.

        Nothing is clicked or solved. A hard denial or rate limit never waits.
        """
        session = self._uc
        if session is None or exc.kind != 'challenge' or session.challenge_grace_seconds <= 0:
            return False
        deadline = time.monotonic() + session.challenge_grace_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            session.idle(min(5.0, max(0.5, remaining)), self._on_wait_tick)
            try:
                check_challenge(self._driver().page_source, self.cfg.anti_bot)
            except AccessChallenge as current:
                if current.kind != 'challenge':
                    return False
                continue
            self._last_http_error = None
            self._http_checked = False
            self.last_response_evidence = {}
            return True

    def snapshot(self, label='inspect') -> tuple[str, str]:
        """Write local HTML/screenshot on explicit request; may contain account data."""
        driver = self._driver()
        safe = ''.join(c if c.isalnum() or c in '-_' else '_' for c in label)[:100]
        self.artifacts.mkdir(parents=True, exist_ok=True)
        html_path = self.artifacts / f'{safe}.html'
        png_path = self.artifacts / f'{safe}.png'
        html_path.write_text(driver.page_source, encoding='utf-8')
        driver.save_screenshot(str(png_path))
        return str(html_path), str(png_path)

    def _manual_verification_enabled(self) -> bool:
        return (
            not self.cfg.selenium.headless
            and self.cfg.selenium.on_challenge == 'pause_for_manual_verification'
            and os.environ.get('TOPCV_CONTAINER_BROWSER') != '1'
        )

    def _pause_for_manual_verification(self, exc: AccessChallenge, *, ready=None) -> None:
        """Wait passively while a human completes verification in the visible Chrome UI."""
        timeout = float(self.cfg.selenium.manual_verification_timeout_seconds or 150)
        poll = max(0.5, float(self.cfg.selenium.verification_poll_interval_seconds or 2))
        deadline = time.monotonic() + timeout
        driver = self._driver()
        # \a rings the terminal bell so the operator notices without watching the screen.
        print(
            f'\a[TopCV] Cloudflare challenge detected ({driver.current_url}). '
            f'Please complete verification manually in the Chrome window within {int(timeout)}s. '
            'Do nothing else in that tab; the crawl resumes by itself afterwards.',
            file=sys.stderr,
            flush=True,
        )
        next_notice = time.monotonic() + 30
        while time.monotonic() < deadline:
            if callable(self._on_wait_tick):
                self._on_wait_tick()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if time.monotonic() >= next_notice:
                print(f'[TopCV] Still waiting for manual verification: {int(remaining)}s left.',
                      file=sys.stderr, flush=True)
                next_notice += 30
            self._verification_sleep(min(poll, remaining), remaining)
            try:
                check_challenge(driver.page_source, self.cfg.anti_bot)
            except AccessChallenge:
                continue
            except WebDriverException:
                break
            if self.observe_http:
                self._last_http_error = None
                self._http_checked = False
                try:
                    self._check_http_access()
                    self._http_checked = bool(self.last_response_evidence)
                except AccessChallenge:
                    pass
            if ready is None or not self.cfg.selenium.require_target_page_after_verification or ready():
                print('[TopCV] Verification accepted; continuing the crawl.', file=sys.stderr, flush=True)
                self._last_http_error = None
                self._http_checked = False
                self.last_response_evidence = {'http_status': 200, 'response_class': 'HOST_AVAILABLE'}
                return
        print(f'[TopCV] No verification within {int(timeout)}s; stopping and holding the source '
              'for operator review.', file=sys.stderr, flush=True)
        exc.reason_code = 'manual_verification_timeout'
        raise exc

    def _check_access(self, *, ready=None, reuse_http=False) -> None:
        """Stop immediately on automated/hard denials; wait passively only in explicit manual UI mode."""
        driver = self._driver()
        http_error = None
        if self.observe_http:
            try:
                if reuse_http and self._http_checked:
                    http_error = self._last_http_error
                else:
                    self._last_http_error = None
                    self._http_checked = False
                    self._check_http_access()
                    self._http_checked = bool(self.last_response_evidence)
            except AccessChallenge as exc:
                self._http_checked = True
                http_error = exc
                self._last_http_error = exc
        try:
            check_challenge(driver.page_source, self.cfg.anti_bot)
            if http_error is not None:
                raise http_error
        except AccessChallenge as exc:
            if http_error is not None and http_error.kind == 'rate_limited':
                exc = http_error
            elif http_error is not None and exc.kind == 'challenge':
                exc.retry_after = http_error.retry_after
                if hasattr(http_error, 'http_status'):
                    exc.http_status = http_error.http_status
            evidence = dict(self.last_response_evidence)
            evidence.update(getattr(exc, 'safe_response', {}) or {})
            evidence['response_class'] = ('HOST_RATE_LIMIT' if exc.kind == 'rate_limited'
                                          else 'HOST_ACCESS_DENIED')
            evidence['reason_code'] = getattr(exc, 'reason_code', exc.kind)
            try:
                html = driver.page_source
                if isinstance(html, str):
                    evidence['body_sha256'] = hashlib.sha256(html.encode('utf-8')).hexdigest()
            except WebDriverException:
                pass
            exc.safe_response = evidence
            if 'http_status' in evidence:
                exc.http_status = evidence['http_status']
            if self._wait_out_challenge(exc):
                return
            if exc.kind in ('challenge', 'access_denied') and self._manual_verification_enabled():
                self._pause_for_manual_verification(exc, ready=ready)
                return
            raise exc

    def _check_http_access(self):
        """Use main-document signals, retaining only bounded public diagnostics."""
        import json
        from src.market_pipeline.http_policy import access_error
        driver = self._driver()
        if self._uc is not None:
            return self._check_http_access_from_page()
        try:
            entries = driver.get_log('performance')
        except (AttributeError, WebDriverException):
            return
        for entry in entries:
            try:
                message = json.loads(entry['message'])['message']
                params = message.get('params', {})
                response = params.get('response', {})
                if (message.get('method') == 'Network.responseReceived'
                        and params.get('type') == 'Document'
                        and response.get('url') == driver.current_url):
                    error = access_error(response.get('status'), response.get('headers', {}))
                    status = response.get('status')
                    self.last_response_evidence = {}
                    if type(status) in (int, float) and 100 <= status <= 599:
                        self.last_response_evidence['http_status'] = int(status)
                    if error:
                        self.last_response_evidence.update(getattr(error, 'safe_response', {}))
                        raise error
                    if status in (404, 410) or (type(status) in (int, float) and 500 <= status <= 599):
                        exc = NavigationError('The public source URL did not return usable content')
                        exc.http_status = int(status)
                        exc.reason_code = 'source_url_unavailable' if status in (404, 410) else 'transient_http_error'
                        exc.safe_response = {'http_status': int(status),
                                             'response_class': 'JOB_LOCAL' if status in (404, 410) else 'TRANSIENT'}
                        raise exc
            except (KeyError, TypeError, json.JSONDecodeError):
                continue

    def _check_http_access_from_page(self):
        """HTTP status of the current document, as Chrome itself recorded it.

        The undetected transport attaches after the page has loaded, so the
        DevTools network log has no entry for the navigation. Response headers
        are not available this way; challenge and denial pages are still told
        apart by their HTML markers in _check_access().
        """
        from src.market_pipeline.http_policy import access_error
        try:
            status = self._driver().execute_script(_DOCUMENT_HTTP_STATUS_JS)
        except WebDriverException:
            return
        if type(status) is not int or not 100 <= status <= 599:
            return  # 0/None: Chrome has no status for this document
        self.last_response_evidence = {'http_status': status}
        error = access_error(status, {})
        if error:
            self.last_response_evidence.update(getattr(error, 'safe_response', {}))
            raise error
        if status in (404, 410) or 500 <= status <= 599:
            exc = NavigationError('The public source URL did not return usable content')
            exc.http_status = status
            exc.reason_code = 'source_url_unavailable' if status in (404, 410) else 'transient_http_error'
            exc.safe_response = {'http_status': status,
                                 'response_class': 'JOB_LOCAL' if status in (404, 410) else 'TRANSIENT'}
            raise exc

    def open_entry(self) -> str:
        """Open the TopCV entry page without silently accepting a search scope."""
        driver = self._driver()
        self.last_response_evidence = {}
        self._last_http_error = None
        self._http_checked = False
        self._goto(self.cfg.source['entry_url'])
        self._check_access()
        return driver.current_url

    def open_public_listing(self, url: str) -> str:
        """Open a supplied, independently verified TopCV results URL.

        This is a single-URL diagnostic, NOT a generated-link search strategy.
        A redirect or changed page scope is checked by the diagnostic caller.
        """
        from .pipeline.discovery import route_query_allowed
        parsed = urlsplit(url)
        if (parsed.scheme != 'https' or parsed.hostname != 'www.topcv.vn'
                or parsed.port not in (None, 443) or parsed.username is not None
                or parsed.password is not None
                or (parsed.query and not route_query_allowed(parsed.query)) or parsed.fragment
                or not parsed.path.startswith('/tim-viec-lam-')):
            raise NavigationError('Only a public TopCV results URL is permitted in this diagnostic.')
        driver = self._driver()
        self.last_response_evidence = {}
        self._last_http_error = None
        self._http_checked = False
        self._goto(url)
        self._check_access()
        return driver.current_url

    def open_observed_result_page(self, url: str, route: str, *, page: int) -> str:
        """Follow only a validated href previously extracted from a page's DOM.

        Unlike M1h's exact first-page diagnostic, page >= 2 may contain a
        genuine page=N parameter. No pagination URL is ever constructed here.
        """
        from .pipeline.discovery import validate_page_url
        if page < 2 or not validate_page_url(url, route, page=page):
            raise NavigationError('Unverified TopCV pagination link; navigation refused.')
        driver = self._driver()
        self.last_response_evidence = {}
        self._last_http_error = None
        self._http_checked = False
        self._goto(url)
        self._check_access()
        if not validate_page_url(driver.current_url, route, page=page):
            raise NavigationError('TopCV changed pagination URL/scope after navigation.')
        return driver.current_url

    def inspect_navigation_controls(self, *, limit: int = 180, stage: str = 'entry', keyword: str = '') -> dict:
        """Read-only, bounded control inventory for calibrating TopCV live UI.

        No input values, cookies, browser storage, full HTML or screenshots
        are included. Selector paths are observations, not stable contracts.
        """
        self._check_access()
        result = self._driver().execute_script(_NAVIGATION_AUDIT_JS, limit, stage, keyword)
        if not isinstance(result, dict) or not isinstance(result.get('controls'), list):
            raise NavigationError('TopCV UI inventory did not return expected data.')
        return result

    def _click_verified_control(self, selector: str, label: str) -> None:
        """Click only user-observed TopCV form controls, not unknown popup options."""
        driver = self._driver()
        self._check_access()
        try:
            element = WebDriverWait(driver, self.wait_seconds).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            element.click()
        except (TimeoutException, WebDriverException) as exc:
            raise NavigationError(f'Could not click {label} in the observed TopCV search form.') from exc

    def open_category_for_calibration(self) -> None:
        self._click_verified_control(S.CATEGORY_TRIGGER, 'category selector')

    def dismiss_location_notice_if_present(self) -> bool:
        """Acknowledge only TopCV's verified informational location modal.

        No login, cookies, terms, CAPTCHA, or other dialog is auto-dismissed.
        The acknowledgment does not select a city or authorize a crawl.
        """
        driver = self._driver()
        self._check_access()
        notices = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_NOTICE_OPEN)
        if not any(item.is_displayed() for item in notices):
            return False
        try:
            button = WebDriverWait(driver, self.wait_seconds).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, S.LOCATION_NOTICE_CONFIRM))
            )
            button.click()
            WebDriverWait(driver, self.wait_seconds).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, S.LOCATION_NOTICE_OPEN))
            )
        except (TimeoutException, WebDriverException) as exc:
            raise NavigationError('Location notice remained open; city cannot be verified.') from exc
        print('LOCATION INTRO: acknowledged TopCV location administration notice.')
        return True

    def open_location_for_calibration(self) -> bool:
        self._click_verified_control(S.LOCATION_TRIGGER, 'location selector')
        # On first visit the UI opens an informational modal, not the picker.
        # Dismiss it and let the operator select actual city/mode in Chrome.
        return self.dismiss_location_notice_if_present()

    @staticmethod
    def _same_ui_label(actual: str, expected: str) -> bool:
        def norm(value: str) -> str:
            value = unicodedata.normalize('NFD', value).casefold()
            return ''.join(c for c in value if unicodedata.category(c) != 'Mn').replace('đ', 'd').strip()
        return norm(actual) == norm(expected)

    def applied_location_label(self) -> str:
        """Only the visible search form label counts; job-card cities do not."""
        driver = self._driver()
        elements = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_APPLIED_LABEL)
        return elements[0].text.strip() if elements else ''

    def select_city_from_observed_picker(self, city: str, mode: str) -> dict:
        """Select a top-level city via its *checkbox*, not its row/chevron.

        Built from the real 2026-10-29 M1d dropdown inventory. Never touch
        arbitrary location text elsewhere on the site, and never assume that
        clicking a city proves it was applied. Fail closed on missing evidence.
        """
        if not city or not mode:
            raise ValueError('City and administrative mode are required.')
        self._check_access()
        driver = self._driver()
        if self._same_ui_label(self.applied_location_label(), city):
            return {'city': city, 'mode': mode, 'verified': True, 'already_applied': True}

        dropdowns = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_PICKER)
        if not any(e.is_displayed() for e in dropdowns):
            raise NavigationError('Location dropdown is not open; no city option was clicked.')

        modes = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_MODE_OPTIONS)
        target_mode = next((e for e in modes if self._same_ui_label(e.text, mode)), None)
        if target_mode is None:
            raise NavigationError(f'Administrative mode {mode!r} missing in location picker.')
        if 'selected' not in (target_mode.get_attribute('class') or '').split():
            try:
                target_mode.click()
            except WebDriverException as exc:
                raise NavigationError('Could not select TopCV location administration mode.') from exc
            modes = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_MODE_OPTIONS)
            if not any(self._same_ui_label(e.text, mode) and
                       'selected' in (e.get_attribute('class') or '').split() for e in modes):
                raise NavigationError('Administrative mode was clicked but did not become selected.')

        # A city row has two different click targets: a checkbox and a chevron
        # that drills into districts. Click only the verified checkbox.
        rows = driver.find_elements(By.CSS_SELECTOR, S.LOCATION_CITY_OPTIONS)
        city_row = None
        for row in rows:
            labels = row.find_elements(By.CSS_SELECTOR, S.LOCATION_CITY_TEXT)
            if any(self._same_ui_label(label.text, city) for label in labels):
                city_row = row
                break
        if city_row is None:
            raise NavigationError(f'City {city!r} not found in the observed TopCV location picker.')
        try:
            box = city_row.find_element(By.CSS_SELECTOR, S.LOCATION_CITY_CHECKBOX)
            box.click()
        except (WebDriverException, LookupError) as exc:
            raise NavigationError(f'Could not click the {city!r} city checkbox.') from exc

        # TopCV can update the city label asynchronously; wait a short bounded
        # interval. The operator may use the same live session if it still fails.
        try:
            WebDriverWait(driver, min(self.wait_seconds, 7)).until(
                lambda _d: self._same_ui_label(self.applied_location_label(), city)
            )
        except TimeoutException as exc:
            raise NavigationError(
                f'Clicked {city!r} checkbox but search-bar location still reads '
                f'{self.applied_location_label()!r}. The city is not verified.'
            ) from exc
        return {'city': city, 'mode': mode, 'verified': True, 'already_applied': False,
                'location_label': self.applied_location_label()}

    def fill_verified_keyword(self, keyword: str) -> None:
        """Type only the intended keyword; retain the original browser event flow."""
        if not keyword or not keyword.strip():
            raise ValueError('Keyword must be nonempty.')
        driver = self._driver()
        self._check_access()
        try:
            element = WebDriverWait(driver, self.wait_seconds).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, S.KEYWORD_INPUT))
            )
            element.clear()
            element.send_keys(keyword)
            if element.get_attribute('value').strip().casefold() != keyword.strip().casefold():
                raise NavigationError('Keyword input did not retain the configured keyword.')
        except (TimeoutException, WebDriverException) as exc:
            raise NavigationError('Cannot fill the verified TopCV keyword input.') from exc

    def submit_verified_search_form(self) -> None:
        """Submit after caller confirms keyword/city/sector evidence, never blindly."""
        self._click_verified_control(S.SEARCH_BUTTON, 'search submit button')

    def confirm_manual_search(self, position: Mapping, location: Mapping) -> str:
        """Require the operator to verify IT and city after UI navigation.

        Confirmation does not prove the site's hidden search parameters: it
        is a deliberate inspection gate, not production-ready auto filtering.
        """
        driver = self._driver()
        print(f'\n[TOPCV MANUAL SEARCH] Keyword: {position["keyword"]!r}; city: {location["search_label"]!r}')
        print(f'Select category: {self.cfg.scope["sector_search_label"]!r}; '
              f'location mode: {self.cfg.scope["location_ui_mode"]!r}.')
        print('Submit the search in the browser; wait until the desired results/no-result screen is visible.')
        print('Verify BOTH the IT category and requested city are actually active in the UI.')
        confirmation = input('Type YES after verifying all filters (anything else cancels): ').strip()
        if confirmation != 'YES':
            raise NavigationError('User did not confirm required TopCV search filters.')
        self._check_access()
        parse_listing_page(driver.page_source, driver.current_url, anti_bot=self.cfg.anti_bot)
        return driver.current_url

    def open_search(self, position: Mapping, location: Mapping, *, manual=False) -> str:
        """Manual navigation only until live keyword/filter/pagination DOM is validated."""
        if not manual:
            raise NavigationError(
                'Automatic keyword/category/city UI is not calibrated. '
                'Inspect the live TopCV search DOM before enabling auto search. '
                'Use manual=True for the tested manual-search inspection path.'
            )
        self.open_entry()
        return self.confirm_manual_search(position, location)

    def inspect_listing_state(self) -> dict:
        """Return a bounded read-only signal of verified results vs unknown DOM.

        The search form, a finished document and a URL are NOT listing evidence.
        Never treat absence of result cards alone as a legitimate zero-job page.
        """
        driver = self._driver()
        probe = driver.execute_script(_LISTING_STATE_JS)
        if not isinstance(probe, dict) or 'verified_structure_present' not in probe:
            raise NavigationError('Could not read the TopCV listing DOM status.')
        return {**probe, 'page_url': driver.current_url}

    def wait_for_listing_ready(self, *, baseline: dict | None = None, timeout_seconds: float | None = None) -> dict:
        """Wait until a verified listing DOM remains stable across three polls.

        Chrome uses page_load_strategy='eager': a legitimate TopCV listing can
        remain at document.readyState='interactive' while already displaying
        cards. Requiring 'complete' was incorrectly rejecting such listings.

        Structural readiness is still NOT proof of search scope or complete
        coverage: callers independently verify URL, title, city and card IDs.
        On same-URL AJAX searches, unchanged baseline results never qualify.
        """
        driver = self._driver()
        self._check_access()
        old = baseline or {}
        timeout = self.wait_seconds if timeout_seconds is None else float(timeout_seconds)
        last: dict = {}
        last_identity: tuple[str, str] | None = None
        stable_polls = 0

        def ready(_driver):
            nonlocal last, last_identity, stable_polls
            last = self.inspect_listing_state()
            state = last.get('document_ready_state')
            verified = last.get('verified_structure_present') is True
            signature = last.get('signature')
            page_url = last.get('page_url')

            # 'loading' is never sufficient. A visible container alone or a
            # ready document without validated cards/empty marker is not enough.
            if (state not in ('interactive', 'complete') or not verified
                    or not isinstance(page_url, str) or not page_url
                    or not isinstance(signature, str) or signature == 'none'):
                last_identity, stable_polls = None, 0
                return False

            # Preserve original stale-result gate for same-URL AJAX searches.
            if (old.get('verified_structure_present')
                    and page_url == old.get('page_url')
                    and signature == old.get('signature')):
                last_identity, stable_polls = None, 0
                return False

            identity = (page_url, signature)
            if identity == last_identity:
                stable_polls += 1
            else:
                last_identity, stable_polls = identity, 1

            # At 0.7-second poll frequency this requires ~1.4 s of evidence,
            # mitigating transient cards from a page still receiving AJAX.
            if stable_polls < 3:
                return False
            return last

        try:
            verified = WebDriverWait(driver, timeout, poll_frequency=0.7).until(ready)
        except TimeoutException:
            self._check_access()  # Never turn a denial/unknown DOM into success.
            return {'ready': False, 'reason': 'unverified_results_after_submit',
                    'last_observation': last or self.inspect_listing_state()}
        self._check_access()
        return {'ready': True, 'reason': 'verified_listing_structure',
                'last_observation': verified}

    def fetch_page_capture(self) -> ListingCapture:
        driver = self._driver()
        self._check_access()
        html, url = driver.page_source, driver.current_url
        captured_at = datetime.now(timezone.utc)
        rejected = []
        items = parse_listing_page(html, url, anti_bot=self.cfg.anti_bot,
                                   allow_partial=True, rejected_cards=rejected)
        return ListingCapture(url, html, items, captured_at, tuple(rejected),
                              self.last_response_evidence.get('http_status'))

    def fetch_page(self) -> tuple[str, list[dict]]:
        capture = self.fetch_page_capture()
        return capture.page_url, capture.items

    def has_next(self) -> bool:
        """Do not invent pagination: search UI next-page DOM is unverified."""
        raise NavigationError('TopCV pagination UI has not been calibrated against live DOM.')

    def next_page(self):
        raise NavigationError('TopCV pagination UI has not been calibrated against live DOM.')

    def fetch_detail_capture(self, job_url: str) -> DetailCapture:
        driver = self._driver()
        identity = normalize_job_link(job_url, 'https://www.topcv.vn')
        if identity is None:
            raise ValueError('Only validated public TopCV job detail URLs are allowed.')
        if self._uc is not None:
            # Undetected transport: one tab, loaded with no driver attached.
            self._goto(job_url)
            return self._read_detail(driver, identity)
        parent = driver.current_window_handle
        opened_via_js = False
        if hasattr(driver, 'execute_script') and isinstance(getattr(driver, 'window_handles', None), (list, tuple)):
            try:
                initial_handles = set(driver.window_handles)
                driver.execute_script("window.open(arguments[0], '_blank');", job_url)
                new_handles = [h for h in driver.window_handles if h not in initial_handles]
                if new_handles:
                    driver.switch_to.window(new_handles[-1])
                    opened_via_js = True
            except Exception:
                opened_via_js = False
        if not opened_via_js:
            driver.switch_to.new_window('tab')
            driver.get(job_url)
        try:
            return self._read_detail(driver, identity)
        finally:
            # Always restore listing tab; if opening tab failed don't close parent.
            if driver.current_window_handle != parent:
                driver.close()
                driver.switch_to.window(parent)

    def _read_detail(self, driver, identity) -> DetailCapture:
        """Verify access and layout of the detail page in the current tab, then parse it."""
        self.last_response_evidence = {}
        self._last_http_error = None
        self._http_checked = False
        def page_ready(_driver):
            check_challenge(driver.page_source, self.cfg.anti_bot)
            return any(driver.find_elements(By.CSS_SELECTOR, css) for css in (
                S.STANDARD_ROOT, S.QUICK_ROOT, S.BRAND_ROOT
            ))

        self._check_access(ready=lambda: page_ready(driver))

        try:
            WebDriverWait(driver, self.wait_seconds).until(page_ready)
        except AccessChallenge:
            self._check_access(reuse_http=True, ready=lambda: any(
                driver.find_elements(By.CSS_SELECTOR, css) for css in
                (S.STANDARD_ROOT, S.QUICK_ROOT, S.BRAND_ROOT)
            ))
        except TimeoutException as exc:
            self._check_access(reuse_http=True, ready=lambda: any(
                driver.find_elements(By.CSS_SELECTOR, css) for css in
                (S.STANDARD_ROOT, S.QUICK_ROOT, S.BRAND_ROOT)
            ))
            raise NavigationError('Timed out waiting for a verified TopCV detail layout.') from exc

        # A familiar DOM is not permission to parse a denied/rate-limited
        # response. Inspect the main-document HTTP status on this path too.
        self._check_access(reuse_http=True)
        html, actual_url = driver.page_source, driver.current_url
        captured_at = datetime.now(timezone.utc)
        parsed = parse_detail_page(html, actual_url, anti_bot=self.cfg.anti_bot,
                                   allow_partial=self.cfg.crawl.get('allow_partial_detail', True))
        actual = normalize_job_link(actual_url, 'https://www.topcv.vn')
        if actual is None or actual[1] != identity[1] or parsed['source_job_id'] != identity[1]:
            raise LayoutChanged('Detail redirected or job ID differs from the requested job.')
        return DetailCapture(actual[0], html, parsed, captured_at,
                             self.last_response_evidence.get('http_status'))

    def fetch_detail(self, job_url: str) -> dict:
        return self.fetch_detail_capture(job_url).parsed
