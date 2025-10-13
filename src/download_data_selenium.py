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
EXPORT_BUTTON_FROM_TOOLTIP = (By.XPATH, '//*[@id="button2_orders_7_export_orders"]')

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


# =========================
#   SHOP SWITCHING (XPath)
# =========================
def _open_user_dropdown_and_get_select():
    """
    Opens the Tippy dropdown and returns the VISIBLE <select> inside the popup
    using XPath with starts-with(@id,'tippy-').
    """
    choose_webshop_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div[2]/div[2]/button'))
    )
    # slight hover helps with some tippy setups
    ActionChains(driver).move_to_element(choose_webshop_btn).pause(0.1).perform()
    choose_webshop_btn.click()  # click is more stable than hover

    # visible tippy → select with the subscription_menu_login handler
    tippy_select_xpath = (
        "//div[starts-with(@id,'tippy-') and not(contains(@style,'display: none'))]"
        "//div[contains(@class,'tippy-content')]"
        "//select[contains(@onchange,'subscription_menu_login')]"
    )

    sels = wait.until(EC.presence_of_all_elements_located((By.XPATH, tippy_select_xpath)))
    for s in sels:
        if s.is_displayed():
            return s

    return wait.until(EC.visibility_of_element_located((By.XPATH, tippy_select_xpath)))


def _collect_shops_from_dropdown():
    """
    Returns a list of (label, text, value_json_str) for all options within optgroups.
    Uses the visible Tippy dropdown.
    """
    sel = _open_user_dropdown_and_get_select()

    shops = []
    groups = sel.find_elements(By.XPATH, ".//optgroup")
    for g in groups:
        label = g.get_attribute("label") or ""
        options = g.find_elements(By.XPATH, ".//option")
        for opt in options:
            text = opt.text.strip()
            value = opt.get_attribute("value")
            if text and text.lower() != "válassz szolgáltatást":
                shops.append((label, text, value))

    # close popup by clicking page background to avoid overlay issues
    driver.find_element(By.TAG_NAME, "body").click()
    return shops


# =========================
#  HIDDEN TEMPLATE FALLBACK
# =========================
def _collect_shops_from_hidden_template():
    """
    Reads options from the hidden #user_menu template (bypassing Tippy timing).
    Returns [(label, text, value_json_str), ...]
    """
    hidden_select = wait.until(
        EC.presence_of_element_located((By.XPATH, "//*[@id='user_menu']//select[contains(@onchange,'subscription_menu_login')]"))
    )
    shops = []
    for g in hidden_select.find_elements(By.XPATH, ".//optgroup"):
        label = g.get_attribute("label") or ""
        for opt in g.find_elements(By.XPATH, ".//option"):
            text = opt.text.strip()
            value = opt.get_attribute("value")
            if text and text.lower() != "válassz szolgáltatást":
                shops.append((label, text, value))
    return shops


def _switch_to_shop(shop_visible_text: str):
    old_root = driver.find_element(By.TAG_NAME, "html")

    sel = _open_user_dropdown_and_get_select()
    Select(sel).select_by_visible_text(shop_visible_text)

    # ensure onchange runs
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", sel
    )

    try:
        wait.until(EC.staleness_of(old_root))
    except TimeoutException:
        time.sleep(0.8)

    wait.until(EC.element_to_be_clickable(ORDERS_BUTTON))

def _slug_from_domain(domain_text: str) -> str:
    return domain_text.split(".", 1)[0]


def download_other_webshop_orders() -> None:
    """
    Build the shop list (hidden-template first; if fails, use visible popup via XPath),
    then iterate: switch → download → next. Excludes a few domains.
    """
    exclude_webshops: list[str] = "aquadragons.hu moluk.hu ugears.hu".split()
    visited: set[str] = set()

    time.sleep(0.2)
    html = driver.find_element('tag name', 'html')
    html.send_keys(Keys.PAGE_UP)
    html.send_keys(Keys.PAGE_UP)
    time.sleep(0.2)

    webshop_btn = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="user_button"]'))
    )

    ActionChains(driver).move_to_element(webshop_btn).perform()

    webshop_optgroup = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="tippy-4"]/div/div/div/div[2]/label/select'))
    )

    # todo: get all webshops what is needed, maybe loop through optgroup
    sel = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="tippy-4"]/div/div/div/div[2]/label/select/optgroup/option[3]'))
    )

    sel.click()

    open_orders_and_download_data("okostojasjatek")


def download_reflexshop_orders() -> None:
    open_orders_and_download_data("reflexshop")


def open_orders_and_download_data(webshop: str) -> None:
    order_btn = wait.until(EC.visibility_of_element_located(ORDERS_BUTTON))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", order_btn)
    ActionChains(driver).move_to_element(order_btn).perform()

    driver.get('https://shop.unas.hu/admin_order_export.php')

    time.sleep(0.2)
    html = driver.find_element('tag name', 'html')
    html.send_keys(Keys.PAGE_DOWN)
    html.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.2)

    select_data_type = wait.until(EC.element_to_be_clickable(EXPORT_DATA_TYPE_SELECT))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select_data_type)

    daily_stats(select_data_type=select_data_type, webshop=webshop)

    move_daily_to_other_dir(webshop)

    # year_stats(select_data_type=select_data_type, webshop=webshop)


def daily_stats(webshop: str, select_data_type=None) -> None:
    Select(select_data_type).select_by_index(1)  # first option

    start_date = date(2025, 10, 11)
    with open("../start_date.txt", "w") as f:
        f.write(start_date.strftime("%Y-%m-%d"))

    end_date = date.today()

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    for date_ in dates:
        set_date(date_, date_)
        select_xlsx_format()
        download_file()

    move_daily_to_other_dir(webshop)


def move_daily_to_other_dir(webshop: str) -> None:
    for file in os.listdir(os.getenv("DOWNLOAD_DIR")):
        if file.endswith(".xlsx") and file.find(webshop) != -1:
            os.makedirs(os.path.join(os.getenv("DOWNLOAD_DIR"), "days", webshop), exist_ok=True)

            os.rename(
                os.path.join(os.getenv("DOWNLOAD_DIR"), file),
                os.path.join(os.getenv("DOWNLOAD_DIR"), "days", webshop, file)
            )


def is_excel_files_exist() -> bool:
    for file in os.listdir(os.getenv("DOWNLOAD_DIR")):
        if file.endswith(".xlsx"):
            return True
    return False


def year_stats(webshop: str, select_data_type=None) -> None:
    Select(select_data_type).select_by_index(0)

    today = datetime.now()
    from_october_or_january = datetime(year=today.year, month=10 if today.year == 2025 else 1, day=1)

    while is_excel_files_exist():
        move_daily_to_other_dir(webshop)

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
    # download_reflexshop_orders()
    time.sleep(2)  # small breather if you like
    download_other_webshop_orders()


if __name__ == "__main__":
    main()
