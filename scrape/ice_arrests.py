from selenium import webdriver
import time
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from scrape.config import STATES


def main():
    option = webdriver.ChromeOptions()
    option.add_argument("--incognito")

    browser = webdriver.Chrome(executable_path="/Library/Application Support/Google/chromedriver",
                               chrome_options=option)

    browser.get("https://trac.syr.edu/phptools/immigration/arrest/")
    time.sleep(8)

    full_df = pd.DataFrame()
    for state in STATES.keys():
        print(f"Trying {state}...")
        try:
            state_df, browser = get_state(state, browser)
            full_df = full_df.append(state_df)
            print(f"{state} was successful!")
            time.sleep(8)
        except NoSuchElementException:
            print(f"{state} not available.")

    full_df = full_df.loc[(full_df.MonthYear != "All") & (full_df.County != "All")]
    full_df.to_csv("data/all_states_ice_arrest.csv", index=False)


def get_state(state_name, browser):

    browser.find_element_by_link_text(state_name).click()

    time.sleep(8)

    soup = BeautifulSoup(browser.page_source, 'lxml')
    soup.prettify()

    df = pd.DataFrame()
    for match in soup.find_all("div", class_="scroll", id="col2"):
        table_body = match.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            df = df.append(pd.Series([ele for ele in cols if ele]), ignore_index=True)
    df = df.rename(columns={0: "County", 1: "Arrests"})
    county_list = df.copy().County.values

    menu = Select(browser.find_element(
        By.XPATH, "//div[@id='col3head2']/select[@id='dimension_pick_col1']"))
    for item in menu.options:
        this = item.get_attribute("innerText")
        if this == "Month and Year":
            menu.select_by_visible_text("Month and Year")

    time.sleep(8)

    all_counties = pd.DataFrame()
    for county in county_list:
        print(f"Getting {county}...")
        df_county, browser = get_county(county, browser)
        print(f"{county} successful!")
        all_counties = all_counties.append(df_county)

    all_counties.loc[:, "Arrests"] = all_counties.Arrests.str.replace(",", "").astype(int)
    all_counties = all_counties.assign(state=state_name)
    all_counties.to_csv(f"data/{state_name}_arrests.csv", index=False)
    return all_counties, browser


def get_county(county_name, browser):
    browser.find_element_by_link_text(county_name).click()
    time.sleep(5)
    soup = BeautifulSoup(browser.page_source, 'lxml')
    soup.prettify()
    county_df = pd.DataFrame()
    for match in soup.find_all("div", class_="scroll", id="col3"):
        table_body = match.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            county_df = county_df.append(pd.Series([ele for ele in cols if ele]), ignore_index=True)
    county_df = county_df.rename(columns={0: "MonthYear", 1: "Arrests"})
    county_df = county_df.assign(County=county_name)

    return county_df, browser
