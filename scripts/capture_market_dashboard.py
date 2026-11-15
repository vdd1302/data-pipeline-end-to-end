"""Verify the rendered local Superset dashboard using a private admin session.

The session is transferred through process stdin and never written to reports.
No user account or password is created or changed.
"""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SUPERSET = 'topcv-a61d-superset-isolated-superset-1'
BROWSER = 'topcv-market-e2e-market-scheduler-1'


def main():
    session_code = '''
from superset.app import create_app
app=create_app()
with app.app_context():
 from superset import security_manager
 admin=security_manager.find_user(username='admin')
 value=app.session_interface.get_signing_serializer(app).dumps({'_user_id':str(admin.id),'_fresh':True})
 print('PRIVATE_SESSION='+value)
'''
    session = subprocess.run(['docker','exec','-i',SUPERSET,'python'],input=session_code,
        capture_output=True,text=True,encoding='utf-8',timeout=60)
    values = [line.split('=',1)[1] for line in session.stdout.splitlines() if line.startswith('PRIVATE_SESSION=')]
    if session.returncode or not values:
        raise RuntimeError('Could not establish local Superset validation session')
    browser_code = '''
import json,time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
options=webdriver.ChromeOptions()
options.binary_location='/usr/bin/chromium'
for arg in ('--headless=new','--no-sandbox','--disable-dev-shm-usage','--window-size=1680,2400'):
 options.add_argument(arg)
options.set_capability('goog:loggingPrefs',{'browser':'ALL'})
driver=webdriver.Chrome(service=Service('/usr/bin/chromedriver'),options=options)
try:
 driver.get('http://host.docker.internal:8090/login/')
 driver.add_cookie({'name':'session','value':PRIVATE_COOKIE,'path':'/'})
 driver.get('http://host.docker.internal:8090/superset/dashboard/topcv-market/')
 for _ in range(60):
  time.sleep(1)
  text=driver.find_element('tag name','body').text
  if ('Market |' in text and 'Waiting on ' not in text and _ >= 25
      and not driver.find_elements('css selector','.ant-spin-spinning')):
   break
 driver.save_screenshot('/tmp/market-dashboard.png')
 text=driver.find_element('tag name','body').text
 errors=driver.get_log('browser')
 result={'url_path':'/superset/dashboard/topcv-market/',
   'dashboard_title_visible':'TopCV' in driver.title or 'TopCV' in text,
   'chart_titles_visible':text.count('Market |'),
   'login_required':'/login/' in driver.current_url,
   'chart_error_visible':any(marker in text for marker in ('Unexpected error','Unable to load','An error occurred','Data error','Duplicate column')),
   'still_loading':'Waiting on ' in text,
   'browser_error_count':sum(e.get('level')=='SEVERE' for e in errors),
   'browser_errors':[e['message'][:220] for e in errors if e.get('level')=='SEVERE'][:10]}
 print('UI_RESULT='+json.dumps(result))
finally:
 driver.quit()
'''
    browser_code = 'PRIVATE_COOKIE=' + repr(values[0]) + '\n' + browser_code
    result = subprocess.run(['docker','exec','-i',BROWSER,'python'],input=browser_code,
        capture_output=True,text=True,encoding='utf-8',timeout=120)
    reports = [line.split('=',1)[1] for line in result.stdout.splitlines() if line.startswith('UI_RESULT=')]
    if result.returncode or not reports:
        raise RuntimeError('Dashboard browser verification failed; private session was not logged')
    report = json.loads(reports[-1])
    destination = ROOT/'artifacts/market/dashboard.png'
    subprocess.run(['docker','cp',BROWSER+':/tmp/market-dashboard.png',str(destination)],
                    check=True,capture_output=True)
    (ROOT/'artifacts/market/dashboard-ui.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2))
    return 0 if report['dashboard_title_visible'] and not report['login_required'] and not report['chart_error_visible'] and not report['still_loading'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
