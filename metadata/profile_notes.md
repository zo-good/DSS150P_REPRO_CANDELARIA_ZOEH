## Findings, risks and ownership (written by hand)

### Ownership and availability
- **Dengue file:** published on HDX by Cirrolytix (DOH Epidemiology Bureau weekly surveillance data). Static snapshot, last modified 2022-05-02, no further updates expected. CC BY 4.0.
- **Weather files:** Open-Meteo-format daily weather per city. Static snapshot, exact download page not recorded by the original group.
- **Privacy:** no personal data. All values are weekly or daily totals per city, so the files are safe to publish.

### Candidate keys
- Dengue: `(loc, date)` is unique (0 duplicates).
- Weather 2010-2019: `(city_name, datetime)` is unique.
- Weather 2020-2023: `(city_name, datetime)` is **not** unique (see risk 6).

### Schema and quality risks
1. **HXL tag row.** Row 1 below the header is `#adm2+name, #affected+infected, ...`, an HDX labelling convention, not data. It forces `cases`, `deaths` and `date` to load as text. It is quarantined in staging.
2. **Date format** is `M/D/YYYY`. Every date is a **Sunday**, so each row is one epidemiological week, labelled by its last day.
3. **One missing death count:** QUEZON CITY, 2017-05-07. The group filled it with 0.
4. **Mixed location levels.** `loc` mixes provinces and cities (126 places). Only 41 cities match the weather data, through a name mapping (`MANILA CITY` -> `Manila`). The mapping lives in config, not in code.
5. **Uneven coverage.** Places have 208 to 261 weeks each, so not every place has every week.
6. **Duplicate weather rows.** In the 2020-2023 file, Lipa City, Lucena, Maasin and Mabalacat City each appear twice per day with slightly different values (5,844 extra rows). **Lucena is a model city.** The group's weekly aggregation summed both copies, so Lucena's 2020 weekly rainfall is about double the true value.
7. **Different weather schemas.** 24 columns vs 22: `latitude` and `longitude` are missing from 2020 onward, so the duplicate cities cannot be told apart by location.
8. **Constant column.** `snowfall_sum` is always 0 and carries no information.
9. **Extreme but real value.** A wind gust of 316 km/h (Baybay and Tabaco, 2019-12-03) matches Typhoon Kammuri (Tisoy). Kept, not treated as an error.

### Leakage risks (what must NOT be a model feature)
- `cases` for the same week: the severity label is computed from it. Only past weeks (`cases_lag1`, `cases_lag2`, `cases_lag3`) are allowed.
- `cases_next` is the regression target.
- `deaths` for the same week is reported together with or after cases.
- Severity thresholds (33rd/66th percentile per city) must come from the **training period only**.