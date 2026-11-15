"""Opt-in undetected-chromedriver transport for TopCVCrawler.

Operator decision of 2026-11-13. The default transport is still plain Selenium
and is unchanged; nothing here runs unless the operator starts a crawl with
``--browser-driver undetected`` (or ``TOPCV_BROWSER_DRIVER=undetected``).

What this transport does differently:

* Chrome is started by undetected-chromedriver, so pages do not see
  ``navigator.webdriver`` or the ``cdc_`` properties that a stock chromedriver
  adds to every document (also when it attaches to a running Chrome).
* A page is loaded while no driver is attached. The navigation is scheduled in
  the page, chromedriver is stopped, the page loads by itself, and chromedriver
  is attached again only to read the finished document.

What it deliberately does not do: solve CAPTCHA/Turnstile, rotate IPs or
proxies, spoof a user agent, or continue after a hard denial. Pacing, the
host gate and stop-on-denial are still enforced by ``market_pipeline.crawl``.
"""
from __future__ import annotations

import gc
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

from selenium.common.exceptions import (
    InvalidSessionIdException, SessionNotCreatedException, TimeoutException, WebDriverException,
)

DRIVER_SELENIUM = 'selenium'
DRIVER_UNDETECTED = 'undetected'
DRIVERS = frozenset({DRIVER_SELENIUM, DRIVER_UNDETECTED})
DEFAULT_SETTLE_SECONDS = 6.0
DEFAULT_CHALLENGE_GRACE_SECONDS = 20.0
MAX_SETTLE_SECONDS = 30.0
MAX_CHALLENGE_GRACE_SECONDS = 120.0
PROFILE_SUBDIRECTORY = 'uc-profile'

# The navigation starts after chromedriver has gone, so the request and the
# page's own scripts run in a browser that nothing is attached to.
_NAVIGATION_DELAY_MS = 400
_SCHEDULE_NAVIGATION_JS = (
    'const target = arguments[0];'
    'setTimeout(function () { window.location.assign(target); }, arguments[1]);'
)
# performance.timeOrigin is unique per document: it tells a new document from
# the previous one even when both have the same URL, without touching the page.
_DOCUMENT_STATE_JS = (
    'return {origin: performance.timeOrigin, ready: document.readyState, href: location.href};'
)
_INSTALL_HINT = 'python -m pip install undetected-chromedriver setuptools'


class BrowserStartError(RuntimeError):
    """Chrome could not be started; the message is safe to show the operator."""

    def __init__(self, message):
        super().__init__(message)
        self.operator_hint = message


def _first_line(error):
    lines = str(error).strip().splitlines()
    return re.sub(r'\s+', ' ', lines[0])[:200] if lines else type(error).__name__


def _windows_chrome_major(binary):
    """chrome.exe prints no version; its version folder and the registry do."""
    versions = []
    try:
        for child in Path(binary).parent.iterdir():
            if child.is_dir() and re.fullmatch(r'\d+(\.\d+){3}', child.name):
                versions.append(tuple(int(part) for part in child.name.split('.')))
    except OSError:
        pass
    if versions:
        return max(versions)[0]
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r'Software\Google\Chrome\BLBeacon') as key:
            value, _kind = winreg.QueryValueEx(key, 'version')
        return int(str(value).split('.')[0])
    except (ImportError, OSError, ValueError):
        return None


def chrome_major_version(binary):
    """Major version of the installed browser, so the matching driver is used."""
    if not binary:
        return None
    if sys.platform == 'win32':
        return _windows_chrome_major(binary)
    try:
        output = subprocess.run([str(binary), '--version'], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    found = re.search(r'(\d+)\.\d+\.\d+', output or '')
    return int(found[1]) if found else None


def _patchable_driver_copy(source):
    """undetected-chromedriver patches the driver file in place; never the system copy."""
    path = Path(source)
    if not path.is_file():
        raise BrowserStartError(f'CHROMEDRIVER_PATH does not point to a file: {source}')
    stat = path.stat()
    directory = Path(tempfile.gettempdir()) / 'topcv-uc-driver'
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f'{stat.st_size}-{int(stat.st_mtime)}-{path.name}'
    if not target.exists():
        partial = target.with_name(target.name + f'.{os.getpid()}.tmp')
        shutil.copy2(path, partial)
        partial.replace(target)
    return str(target)


class UndetectedSession:
    """One Chrome started by undetected-chromedriver, with detached page loads."""

    def __init__(self, driver, *, settle_seconds, challenge_grace_seconds, page_load_timeout_seconds,
                 capabilities=None, sleep_fn=time.sleep, monotonic_fn=time.monotonic):
        self.driver = driver
        self.settle_seconds = float(settle_seconds)
        self.challenge_grace_seconds = float(challenge_grace_seconds)
        self.page_load_timeout_seconds = float(page_load_timeout_seconds)
        self.attached = True
        self._capabilities = capabilities
        self._sleep_fn, self._clock = sleep_fn, monotonic_fn

    @classmethod
    def start(cls, selenium_cfg, *, in_container=False):
        try:
            import undetected_chromedriver as uc
        except ImportError as exc:
            # Python 3.12+ has no distutils; setuptools provides the module uc still imports.
            raise BrowserStartError(
                f'undetected-chromedriver cannot be imported ({_first_line(exc)}). Run: {_INSTALL_HINT}'
            ) from exc
        if selenium_cfg.headless:
            raise BrowserStartError(
                'The undetected driver needs a visible Chrome (a desktop, or Xvfb in the container); '
                'headless Chrome identifies itself as HeadlessChrome and is refused here.')
        profile = Path(selenium_cfg.browser_profile_dir)
        if re.search(r'lang|headless', str(profile)):
            # undetected-chromedriver scans every Chrome argument, this path included, for these
            # words and would take the path for a language or a headless switch.
            raise BrowserStartError(
                f'The browser profile path must not contain "lang" or "headless": {profile}. '
                'Move the project folder or set selenium.browser_profile_dir to another path.')
        profile.mkdir(parents=True, exist_ok=True)
        binary = (os.environ.get('CHROME_BINARY') or os.environ.get('CHROME_BIN')
                  or uc.find_chrome_executable())
        if not binary or not Path(binary).exists():
            raise BrowserStartError('Google Chrome was not found; install it or set CHROME_BINARY.')
        driver_path = os.environ.get('CHROMEDRIVER_PATH') or None
        if driver_path:
            driver_path = _patchable_driver_copy(driver_path)

        def launch(version_main):
            options = uc.ChromeOptions()
            options.page_load_strategy = 'eager'
            if in_container:
                options.add_argument('--disable-dev-shm-usage')
            return uc.Chrome(options=options, user_data_dir=str(profile), browser_executable_path=str(binary),
                             driver_executable_path=driver_path, version_main=version_main,
                             # Chrome's stderr goes to a pipe nobody reads; keep it nearly silent.
                             use_subprocess=True, log_level=3)

        version = chrome_major_version(binary)
        try:
            try:
                driver = launch(version)
            except SessionNotCreatedException as exc:
                actual = re.search(r'Current browser version is (\d+)', str(exc))
                if driver_path or not actual or int(actual[1]) == version:
                    raise
                # Chrome updated itself: fetch the driver of the version that really runs.
                gc.collect()  # the failed driver object stops the Chrome it had started
                time.sleep(2)
                driver = launch(int(actual[1]))
        except BrowserStartError:
            raise
        except Exception as exc:
            raise BrowserStartError(
                f'Chrome did not start with the undetected driver: {_first_line(exc)}. '
                f'Close every Chrome window that uses {profile}, check that Chrome and its driver '
                'have the same major version, then run again.') from exc
        try:
            session = cls(driver, settle_seconds=selenium_cfg.detached_navigation_seconds,
                          challenge_grace_seconds=selenium_cfg.challenge_grace_seconds,
                          page_load_timeout_seconds=selenium_cfg.page_load_timeout_seconds,
                          capabilities=driver.options.to_capabilities())
            driver.set_page_load_timeout(session.page_load_timeout_seconds)
            session._keep_single_tab()
            return session
        except Exception:
            driver.quit()
            raise

    def _keep_single_tab(self):
        """A restored profile may reopen old tabs; the crawler drives exactly one."""
        handles = self.driver.window_handles
        for handle in handles[1:]:
            self.driver.switch_to.window(handle)
            self.driver.close()
        if handles:
            self.driver.switch_to.window(handles[0])

    def disconnect(self):
        """Stop chromedriver; Chrome keeps running with nothing attached to it."""
        if not self.attached:
            return
        self.attached = False
        try:
            self.driver.service.stop()
        except Exception:
            # A driver process that is already gone is as disconnected as a stopped one.
            pass

    def reconnect(self):
        if self.attached:
            return
        try:
            self.driver.service.start()
            self.driver.start_session(self._capabilities)
            self.driver.set_page_load_timeout(self.page_load_timeout_seconds)
        except Exception as exc:
            # Chrome was closed or crashed. The worker ends the run as browser_unavailable
            # instead of charging a retry to every remaining scope.
            raise InvalidSessionIdException('Could not attach to Chrome again after a detached page load') from exc
        self.attached = True

    def _sleep(self, seconds, tick):
        deadline = self._clock() + max(0.0, seconds)
        while True:
            remaining = deadline - self._clock()
            if remaining <= 0:
                return
            self._sleep_fn(min(5.0, remaining))
            if callable(tick):
                tick()

    def idle(self, seconds, tick=None):
        """Leave Chrome alone for ``seconds`` (page load, or a person verifying), then attach again."""
        self.disconnect()
        try:
            self._sleep(seconds, tick)
        finally:
            self.reconnect()

    def navigate(self, url, tick=None):
        """Load ``url`` in the current tab and return once the new document is readable."""
        driver = self.driver
        if self.settle_seconds <= 0:
            driver.get(url)
            return
        previous = driver.execute_script(_DOCUMENT_STATE_JS)
        driver.execute_script(_SCHEDULE_NAVIGATION_JS, url, _NAVIGATION_DELAY_MS)
        deadline = self._clock() + self.page_load_timeout_seconds
        wait = self.settle_seconds
        while True:
            self.idle(wait, tick)
            state = driver.execute_script(_DOCUMENT_STATE_JS)
            if state['origin'] != previous['origin'] and state['ready'] in ('interactive', 'complete'):
                break
            if self._clock() >= deadline:
                raise TimeoutException('The page did not finish loading within the page load timeout')
            wait = 2.0
        if str(state['href']).startswith('chrome-error://'):
            raise WebDriverException('Chrome could not load the page (network error)')
