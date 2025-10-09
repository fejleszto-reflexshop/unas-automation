import os
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver import Keys, ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from dotenv import load_dotenv

# =========================
#         CONFIG
# =========================
load_dotenv()
UNAS_URL = os.getenv("UNAS_URL")
UNAS_USERNAME_LOGIN = os.getenv("UNAS_USERNAME_LOGIN")
UNAS_PASSWORD_LOGIN = os.getenv("UNAS_PASSWORD_LOGIN")

# Visual speed controls
TYPE_DELAY = 0.12    # seconds between keystrokes when typing dates
VIEW_PAUSE = 0.35    # small pauses so you can see changes

options = Options()
options.add_experimental_option("detach", True)  # keep browser open
options.headless = False

driver = webdriver.Chrome(options=options)
driver.maximize_window()
wait = WebDriverWait(driver, 15)

# =========================
#       LOCATORS
# =========================
COOKIE_BTN = (By.XPATH, '//*[@id="unas"]/div[5]/div[1]/button')
USER_INPUT = (By.XPATH, '//*[@id="login-user"]')
PASS_INPUT = (By.XPATH, '//*[@id="login-pass"]')
SERVICE_SELECT = (By.XPATH, '//*[@id="login-service-username"]')
ENTER_BUTTON = (By.XPATH, '//*[@id="login-form"]/button')

ORDERS_BUTTON = (By.XPATH, '//*[@id="button1_orders_1_0"]')
EXPORT_BUTTON_FROM_TOOLTIP = (By.XPATH, '//*[@id="tippy-20"]/div/div[1]/div[6]')

EXPORT_DATA_TYPE_SELECT = (By.XPATH, '//*[@id="export"]/form/div[8]/div[2]/div/select')
# Your absolute XPaths for the masked HU date inputs:
DATE_START_INPUT = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[1]/label/input')
DATE_END_INPUT   = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[2]/label/input')

CSV_RADIO = (By.XPATH, '//*[@id="format_3"]')
EXPORT_SUBMIT = (By.XPATH, '//*[@id="button_export"]/button')

# =========================
#       HELPERS
# =========================
def open_browser() -> None:
    driver.get(UNAS_URL)

def safe_click(locator, disappear_locator=None, attempts: int = 3):
    """Click an element; if it goes stale, refetch and retry."""
    last_err = None
    for _ in range(attempts):
        try:
            el = wait.until(EC.element_to_be_clickable(locator))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            if disappear_locator:
                wait.until(EC.invisibility_of_element_located(disappear_locator))
            return
        except StaleElementReferenceException as e:
            last_err = e
    if last_err:
        raise last_err

def close_cookies_once():
    try:
        safe_click(COOKIE_BTN, disappear_locator=COOKIE_BTN)
    except TimeoutException:
        pass  # banner not present

def highlight(el, color="#ff4d4f", thickness=3):
    """Briefly highlight an element so it's easy to see what's being edited."""
    driver.execute_script(
        "arguments[0].setAttribute('data-old-style', arguments[0].getAttribute('style') || '');"
        "arguments[0].style.outline = arguments[1];"
        "arguments[0].style.transition = 'outline 0.2s';",
        el, f"{thickness}px solid {color}"
    )
    time.sleep(VIEW_PAUSE)

def unhighlight(el):
    driver.execute_script(
        "arguments[0].style.outline='';",
        el
    )

def type_slow(el, text: str, delay: float = TYPE_DELAY):
    """Type characters one by one with a tiny delay so you can watch it."""
    for ch in text:
        el.send_keys(ch)
        time.sleep(delay)

def set_hu_date(locator, dt):
    """
    Sets a Hungarian masked date input like 'YYYY. MM. DD.' slowly,
    so you can watch the value being entered.
    """
    el = wait.until(EC.element_to_be_clickable(locator))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    highlight(el)

    date_str = dt.strftime("%Y. %m. %d.")  # HU mask with spaces + trailing dot

    el.click()
    time.sleep(VIEW_PAUSE)

    # Clear robustly (masked inputs often ignore .clear())
    el.send_keys(Keys.CONTROL, "a")  # (use Keys.COMMAND on macOS)
    time.sleep(TYPE_DELAY)
    el.send_keys(Keys.DELETE)
    time.sleep(VIEW_PAUSE)

    # Type slowly so it's visible
    type_slow(el, date_str, TYPE_DELAY)
    time.sleep(VIEW_PAUSE)
    el.send_keys(Keys.TAB)  # commit
    time.sleep(VIEW_PAUSE)

    # If mask rejected or didn't update correctly, set via JS and fire events
    if el.get_attribute("value") != date_str:
        driver.execute_script("""
            const el = arguments[0], v = arguments[1];
            el.value = v;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
        """, el, date_str)
        time.sleep(VIEW_PAUSE)

    unhighlight(el)

def login() -> None:
    # Close cookies BEFORE interacting with the form
    close_cookies_once()

    # Username
    user = wait.until(EC.presence_of_element_located(USER_INPUT))
    user.clear()
    user.send_keys(UNAS_USERNAME_LOGIN)

    # Password
    pwd = wait.until(EC.presence_of_element_located(PASS_INPUT))
    pwd.clear()
    pwd.send_keys(UNAS_PASSWORD_LOGIN)
    pwd.send_keys(Keys.RETURN)

    # Wait for service select (page re-renders)
    sel = wait.until(EC.presence_of_element_located(SERVICE_SELECT))

    # Cookie banner may reappear after navigation
    close_cookies_once()

    # Pick service via Select API (avoid sending keys to <option>)
    select_widget = Select(sel)
    select_widget.select_by_index(4)  # adjust if needed

    # Enter
    enter_btn = wait.until(EC.element_to_be_clickable(ENTER_BUTTON))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", enter_btn)
    enter_btn.click()

def open_orders_and_download_data() -> None:
    # Hover Orders
    order_btn = wait.until(EC.visibility_of_element_located(ORDERS_BUTTON))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", order_btn)
    ActionChains(driver).move_to_element(order_btn).perform()

    # Open Export
    export_btn = wait.until(EC.visibility_of_element_located(EXPORT_BUTTON_FROM_TOOLTIP))
    export_btn.click()

    time.sleep(1)
    html = driver.find_element('tag name', 'html')
    html.send_keys(Keys.PAGE_DOWN)
    html.send_keys(Keys.PAGE_DOWN)
    time.sleep(1)

    # Select data type
    select_data_type = wait.until(EC.element_to_be_clickable(EXPORT_DATA_TYPE_SELECT))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select_data_type)
    Select(select_data_type).select_by_index(1)  # 2nd option

    # Set dates in Hungarian format (SLOW & VISIBLE)
    # TODO: fix issue with pasting date into input field
    today = datetime.now().strftime('%Y-%m-%d')

    select_start_date = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="date_start"]'))
    )

    select_start_date.send_keys(today)

    select_start_date.send_keys(Keys.TAB)
    select_start_date.send_keys(Keys.TAB)
    select_start_date.send_keys(Keys.RETURN)
    select_start_date.send_keys(Keys.TAB)
    select_start_date.send_keys(Keys.TAB)
    select_start_date.send_keys(Keys.RETURN)

    # Choose CSV (enable if needed)
    # csv_radio = wait.until(EC.element_to_be_clickable(CSV_RADIO))
    # driver.execute_script("arguments[0].scrollIntoView({block:'center'});", csv_radio)
    # csv_radio.click()

    # Download
    # download_btn = wait.until(EC.element_to_be_clickable(EXPORT_SUBMIT))
    # driver.execute_script("arguments[0].scrollIntoView({block:'center'});", download_btn)
    # download_btn.click()

# =========================
#          MAIN
# =========================
def main() -> None:
    open_browser()
    login()
    open_orders_and_download_data()
    print("✅ Process complete — browser stays open for inspection.")

if __name__ == "__main__":
    main()
