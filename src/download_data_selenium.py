import os
import time
from datetime import datetime, date, timedelta

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

VIEW_PAUSE = 0.35  # small pauses so you can watch changes

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

DATE_START_INPUT = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[1]/label/input')
DATE_END_INPUT   = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[2]/label/input')

XLSX_RADIO = (By.XPATH, '//*[@id="format_1"]')
EXPORT_SUBMIT = (By.XPATH, '//*[@id="button_export"]/button')


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

def highlight(el, color="#ff4d4f"):
    driver.execute_script("arguments[0].style.boxShadow='0 0 0 3px '+arguments[1];", el, color)
    time.sleep(VIEW_PAUSE)

def unhighlight(el):
    driver.execute_script("arguments[0].style.boxShadow='';", el)

def set_date_resilient(locator, dt: datetime, label="date"):
    """
    Robustly set a date input that may be either:
      - <input type="date"> -> needs YYYY-MM-DD
      - masked text input with HU placeholder 'éééé. hh. nn.' -> needs 'YYYY. MM. DD.'
    Handles non-breaking spaces and hidden paired inputs; fires input/change/blur.
    """
    el = wait.until(EC.presence_of_element_located(locator))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    highlight(el)

    input_type = (el.get_attribute("type") or "").lower()
    placeholder = (el.get_attribute("placeholder") or "")
    current_val = el.get_attribute("value") or ""
    uses_nbsp = ("\u00a0" in placeholder) or ("\u00a0" in current_val)
    space_char = "\u00a0" if uses_nbsp else " "

    iso = dt.strftime("%Y-%m-%d")
    hu  = dt.strftime(f"%Y.{space_char} %m.{space_char} %d.")

    # --- Strategy A: native <input type="date">
    if input_type == "date":
        driver.execute_script("""
            const el = arguments[0], v = arguments[1];
            el.focus();
            el.value = v;
            try { el.valueAsDate = new Date(v); } catch(e) {}
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
            el.blur();
        """, el, iso)
        time.sleep(VIEW_PAUSE)

        if (el.get_attribute("value") or "") == iso:
            print(f"[{label}] set as ISO OK ->", iso)
            unhighlight(el)
            return

    # --- Strategy B: masked HU text input
    driver.execute_script("""
        const el = arguments[0], v = arguments[1];
        el.focus();
        el.value = v;
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        el.blur();
    """, el, hu)
    time.sleep(VIEW_PAUSE)

    if (el.get_attribute("value") or "") == hu:
        print(f"[{label}] set as HU masked OK ->", hu)
        unhighlight(el)
        return

    # --- Strategy C: update a hidden paired input next to visible one
    driver.execute_script("""
        const el = arguments[0], hu = arguments[1], iso = arguments[2];
        const root = el.closest('label') || el.parentElement;
        if (root) {
            const hidden = root.querySelector('input[type="hidden"], input[data-hidden="true"]');
            if (hidden) {
                // prefer ISO for model fields
                hidden.value = iso;
                hidden.dispatchEvent(new Event('input', {bubbles:true}));
                hidden.dispatchEvent(new Event('change', {bubbles:true}));
            }
        }
        // also re-apply visible for UX
        el.value = hu;
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        el.blur();
    """, el, hu, iso)
    time.sleep(VIEW_PAUSE)

    v = el.get_attribute("value") or ""
    print(f"[{label}] after hidden-pair attempt -> '{v}'")
    unhighlight(el)

def login() -> None:
    close_cookies_once()

    user = wait.until(EC.presence_of_element_located(USER_INPUT))
    user.clear()
    user.send_keys(UNAS_USERNAME_LOGIN)

    pwd = wait.until(EC.presence_of_element_located(PASS_INPUT))
    pwd.clear()
    pwd.send_keys(UNAS_PASSWORD_LOGIN)
    pwd.send_keys(Keys.RETURN)

    sel = wait.until(EC.presence_of_element_located(SERVICE_SELECT))
    close_cookies_once()

    select_widget = Select(sel)
    select_widget.select_by_index(4)  # adjust if needed

    enter_btn = wait.until(EC.element_to_be_clickable(ENTER_BUTTON))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", enter_btn)
    enter_btn.click()

def open_orders_and_download_data() -> None:
    order_btn = wait.until(EC.visibility_of_element_located(ORDERS_BUTTON))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", order_btn)
    ActionChains(driver).move_to_element(order_btn).perform()

    export_btn = wait.until(EC.visibility_of_element_located(EXPORT_BUTTON_FROM_TOOLTIP))
    export_btn.click()

    time.sleep(0.5)
    html = driver.find_element('tag name', 'html')
    html.send_keys(Keys.PAGE_DOWN)
    html.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.5)

    select_data_type = wait.until(EC.element_to_be_clickable(EXPORT_DATA_TYPE_SELECT))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select_data_type)

    daily_stats(select_data_type=select_data_type)

    move_daily_to_other_dir()

    year_stats(select_data_type=select_data_type)


def daily_stats(select_data_type=None) -> None:
    Select(select_data_type).select_by_index(1)  # first option

    start_date = date(2025, 10, 5)
    with open("../start_date.txt", "w") as f:
        f.write(start_date.strftime("%Y-%m-%d"))

    end_date = date.today()

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    for date_ in dates:
        set_date(date_, date_)
        select_xlsx_format()
        download_file()

    move_daily_to_other_dir()

def move_daily_to_other_dir() -> None:
    for file in os.listdir(os.getenv("DOWNLOAD_DIR")):
        if file.endswith(".xlsx"):
            os.makedirs(os.path.join(os.getenv("DOWNLOAD_DIR"), "days"), exist_ok=True)

            os.rename(os.path.join(os.getenv("DOWNLOAD_DIR"), file), os.path.join(os.getenv("DOWNLOAD_DIR"), "days", file))

def is_excel_files_exist() -> bool:
    for file in os.listdir(os.getenv("DOWNLOAD_DIR")):
        if file.endswith(".xlsx"):
            return True

    return False

def year_stats(select_data_type=None) -> None:
    Select(select_data_type).select_by_index(0)

    today = datetime.now()
    from_october_or_january = datetime(year=today.year, month=10 if today.year == 2025 else 1, day=1)

    while is_excel_files_exist():
        move_daily_to_other_dir()

    set_date(date_start=from_october_or_january, date_end=today)
    select_xlsx_format()
    download_file()


def set_date(date_start, date_end) -> None:
    """ just paste datetime.now() """
    set_date_resilient(DATE_START_INPUT, date_start, label="start")
    set_date_resilient(DATE_END_INPUT, date_end, label="end")

def select_xlsx_format() -> None:
    xlsx_radio = wait.until(EC.element_to_be_clickable(XLSX_RADIO))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", xlsx_radio)
    xlsx_radio.click()

def download_file() -> None:
    download_btn = wait.until(EC.element_to_be_clickable(EXPORT_SUBMIT))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", download_btn)
    download_btn.click()

# =========================
#          MAIN
# =========================
def main() -> None:
    open_browser()
    login()
    open_orders_and_download_data()


if __name__ == "__main__":
    main()
