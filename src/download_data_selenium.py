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

load_dotenv()
UNAS_URL = os.getenv("UNAS_URL")
UNAS_USERNAME_LOGIN = os.getenv("UNAS_USERNAME_LOGIN")
UNAS_PASSWORD_LOGIN = os.getenv("UNAS_PASSWORD_LOGIN")

options = Options()
options.add_argument("--headless")

driver = webdriver.Chrome(options=options)
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
EXPORT_BUTTON_FROM_TOOLTIP = (By.XPATH, '//*[@id="button2_orders_7_export_orders"]')

EXPORT_DATA_TYPE_SELECT = (By.XPATH, '//*[@id="export"]/form/div[8]/div[2]/div/select')

DATE_START_INPUT = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[1]/label/input')
DATE_END_INPUT   = (By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div[4]/div/div/form/div[9]/div[2]/div[2]/label/input')

XLSX_RADIO = (By.XPATH, '//*[@id="format_1"]')
EXPORT_SUBMIT = (By.XPATH, '//*[@id="button_export"]/button')

def open_browser() -> None:
    print("Opening browser...", UNAS_URL, sep='\n')
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

    v = el.get_attribute("value") or ""
    print(f"[{label}] after hidden-pair attempt -> '{v}'")
    unhighlight(el)


def login() -> None:
    close_cookies_once()

    print("Logging in...")

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


TOOLTIP_VISIBLE_SELECT = (
    By.XPATH,
    "//div[contains(@class,'tippy-box') and contains(@data-state,'visible')]//label/select"
)

USER_MENU_BTN = (By.ID, "user_button")  # simplify

def open_user_menu() -> None:
    btn = wait.until(EC.visibility_of_element_located(USER_MENU_BTN))
    ActionChains(driver).move_to_element(btn).perform()
    wait.until(EC.presence_of_element_located(TOOLTIP_VISIBLE_SELECT))

    print("User menu opened.")

def list_other_webshops(exclude_webshops: list[str]) -> list[str]:
    """
    Return a deduped list of webshop names (strings) from the visible optgroup,
    excluding any in 'exclude_webshops'.
    """
    open_user_menu()
    sel_el = wait.until(EC.presence_of_element_located(TOOLTIP_VISIBLE_SELECT))
    sel = Select(sel_el)

    names: list[str] = []
    for opt in sel.options:
        txt = (opt.text or "").strip()
        if txt and txt.lower() not in {e.lower() for e in exclude_webshops}:
            names.append(txt)

    # keep order, remove dupes
    seen = set()
    out = []
    for n in names:
        if n.lower() not in seen:
            seen.add(n.lower())
            out.append(n)

    return out

def select_webshop_by_text(name: str, retries: int = 3) -> None:
    """
    Re-open the menu and select the webshop by visible text, handling staleness.
    """
    last_err = None
    for _ in range(retries):
        try:
            open_user_menu()
            sel_el = wait.until(EC.element_to_be_clickable(TOOLTIP_VISIBLE_SELECT))
            Select(sel_el).select_by_visible_text(name)
            time.sleep(0.2)
            return
        except (StaleElementReferenceException, TimeoutException) as e:
            last_err = e
            time.sleep(0.2)
    if last_err:
        raise last_err

def download_other_webshop_orders() -> None:
    exclude_webshops = ["aquadragons.hu", "moluk.hu", "ugears.hu"]
    visited: set[str] = set()

    targets = list_other_webshops(exclude_webshops=exclude_webshops)

    for idx, name in enumerate(targets):
        if name in visited:
            continue
        visited.add(name)

        select_webshop_by_text(name)
        open_orders_and_download_data(name)


def open_orders_and_download_data(webshop: str) -> None:
    driver.get('https://shop.unas.hu/admin_order_export.php')

    print("Downloading orders...")

    time.sleep(0.2)
    html = driver.find_element('tag name', 'html')
    html.send_keys(Keys.PAGE_DOWN)
    html.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.2)

    select_data_type = wait.until(EC.element_to_be_clickable(EXPORT_DATA_TYPE_SELECT))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select_data_type)

    daily_stats(select_data_type=select_data_type, webshop=webshop)

    year_stats(select_data_type=select_data_type, webshop=webshop)


def daily_stats(webshop: str, select_data_type=None) -> None:
    Select(select_data_type).select_by_index(1)  # daily
    start_date = date(2025, 10, 13)
    with open("../start_date.txt", "w") as f:
        f.write(start_date.strftime("%Y-%m-%d"))
    end_date = date.today()
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    for date_ in dates:
        set_date(date_, date_)
        select_xlsx_format()
        download_file()

    print("Daily stats exported.", webshop, sep='\t')


def year_stats(webshop: str, select_data_type=None) -> None:
    Select(select_data_type).select_by_index(0)  # yearly
    today = datetime.now()
    from_october_or_january = datetime(year=today.year, month=10 if today.year == 2025 else 1, day=1)

    set_date(date_start=from_october_or_january, date_end=today)
    select_xlsx_format()
    download_file()

    print("Year stats exported.", webshop, sep='\t')


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


def main() -> None:
    print("Start!")

    open_browser()
    login()
    download_other_webshop_orders()

    print("All done!")


if __name__ == "__main__":
    try:
        main()
    finally:
        driver.quit()
