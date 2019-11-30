import pandas as pd
from pandas.tseries.offsets import MonthEnd
from scrape.config import STATES
import csv
import json


def clean_for_db(df, lat_key):
    df = df.loc[df.County != "All"]
    df = df.loc[df.MonthYear != "All"]
    df = df.assign(
        startTime=df.MonthYear.apply(lambda x: pd.to_datetime(x).isoformat()),
        endTime=(df.MonthYear.apply(lambda z: pd.to_datetime(z) + MonthEnd()))
    )
    df = pd.merge(df, lat_key, on="County")
    df = df.assign(
        endTime=df.endTime.apply(lambda y: y.isoformat()),
        numArrests=df.Arrests.astype(float),
        locationName=df.County
    )
    df = df.loc[:, ["startTime", "endTime", "numArrests", "latitude", "longitude", "locationName"]]
    return df


def prep_for_map(path_to_scraped_data, path_to_geo_key, out_path):
    df = (pd.read_csv(path_to_scraped_data)
          .loc[lambda x: x.County != "All"]
          .groupby(["County", "state"], as_index=False).sum()
          .assign(state_abb=lambda x: x.state.apply(lambda y: STATES[y]),
                  Area_name=lambda x: x.County.apply(lambda y: y.split(',')[0]))
          )
    df = df.assign(GEO=df.Area_name + ', ' + df.state)
    key = df.loc[:, ["GEO", "Area_name", "state_abb", "state", "County"]].drop_duplicates()
    ann = pd.read_csv(path_to_geo_key,
                      usecols=["GEO.id", "GEO.display-label"],
                      encoding="iso-8859-1")
    ann = ann.rename(columns={"GEO.display-label": "GEO"})
    key = pd.merge(ann, key, on="GEO", how="left")
    for_map = df.loc[:, ["Arrests", "GEO"]]
    for_map = for_map.loc[~for_map.Arrests.isna()]
    for_map = pd.merge(key[["GEO", "GEO.id"]], for_map, on="GEO", how="left").fillna(0)
    for_map = for_map.rename(columns={"GEO.id": "GEO_ID"})
    for_map.to_csv(f"{out_path}map_data_ice_arrests.csv", index=False)


def main():
    # migrahack mvp
    arrests = pd.read_csv('month_year_co_counties.csv')
    lat_key = pd.read_csv('centroids.csv')
    new = clean_for_db(arrests, lat_key)

    new.to_csv('final_arrests.csv', index=False)

    csvfile = open('final_arrests.csv', 'r')
    jsonfile = open('final_arrests3.json', 'w')

    fieldnames = ("startTime", "endTime", "numArrests", "latitude", "longitude", "locationName")
    reader = csv.DictReader(csvfile, fieldnames)
    for row in reader:
        print(row)
        json.dump(row, jsonfile)
        jsonfile.write(',\n')


if __name__ == "__main__":
    prep_for_map("~/dta/ice/all_states_ice_arrest.csv",
                 "~/dta/geographic/locations/counties_GEOID_key.csv",
                 "~/dta/ice/")